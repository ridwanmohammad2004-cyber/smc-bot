import os
import time
import json
import threading
import requests
import logging
from datetime import datetime, timezone

try:
    import websocket
except ImportError:
    websocket = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
ANALYZE_INTERVAL   = int(os.environ.get("ANALYZE_INTERVAL", "60"))  # 60s = 1 candle cycle
USE_WEBSOCKET      = os.environ.get("USE_WEBSOCKET", "true").lower() == "true"
METAAPI_TOKEN      = os.environ.get("METAAPI_TOKEN", "")
METAAPI_ACCOUNT_ID = os.environ.get("METAAPI_ACCOUNT_ID", "")

# ─── AUTO-TRADE CONFIG ────────────────────────────────────
AUTO_TRADE_ENABLED  = True
AUTO_TRADE_SYMBOL   = "XAUUSD"

# 3 equal positions, each with own TP
AUTO_TP1_MULT = 0.8   # Easy first target
AUTO_TP2_MULT = 1.5   # Medium
AUTO_TP3_MULT = 2.5   # Runner

# ─── SAFETY FEATURES ──────────────────────────────────────
RISK_PCT           = 0.01   # 1% account risk per trade
FALLBACK_LOT       = 0.01
MIN_LOT            = 0.01
MAX_LOT            = 0.10
MAX_DAILY_LOSSES   = 3
MAX_OPEN_POSITIONS = 1      # 1 signal at a time (= 3 positions)

# MetaAPI
META_API_URL = "https://mt-client-api-v1.london.agiliumtrade.ai"

# ─── INSTRUMENTS ──────────────────────────────────────────
INSTRUMENTS = [
    {"id": "XAUUSD", "label": "Gold",    "symbol": "XAU/USD",  "td_symbol": "XAU/USD",
     "decimals": 2,  "sl_dist": 3.5,    "lot_strong": 0.05, "lot_mid": 0.02,
     "valid_min": 1000,  "valid_max": 10000},
    {"id": "NAS100", "label": "Nasdaq",  "symbol": "US100",    "td_symbol": "NDX",
     "decimals": 1,  "sl_dist": 20.0,   "lot_strong": 0.2,  "lot_mid": 0.1,
     "valid_min": 10000, "valid_max": 40000},
    {"id": "EURUSD", "label": "EUR/USD", "symbol": "EUR/USD",  "td_symbol": "EUR/USD",
     "decimals": 5,  "sl_dist": 0.0015, "lot_strong": 0.03, "lot_mid": 0.02,
     "valid_min": 0.5,   "valid_max": 2.0},
    {"id": "USOUSD", "label": "WTI Oil", "symbol": "WTI/USD",  "td_symbol": "WTI/USD",
     "decimals": 2,  "sl_dist": 0.8,    "lot_strong": 0.03, "lot_mid": 0.02,
     "valid_min": 30,    "valid_max": 130, "buy_only": True},
]
INST_BY_ID = {i["id"]: i for i in INSTRUMENTS}

# ─── STATE ────────────────────────────────────────────────
# Candle store: {inst_id: [{"o","h","l","c","t"}, ...]}  — last 50 closed 1M candles
candle_store     = {i["id"]: [] for i in INSTRUMENTS}
latest_price     = {i["id"]: None for i in INSTRUMENTS}
last_signals     = {i["id"]: None for i in INSTRUMENTS}
last_signal_time = {i["id"]: None for i in INSTRUMENTS}
open_signals     = {}
auto_trades      = {}
auto_trade_history = []

COOLDOWNS = {"XAUUSD": 420, "NAS100": 600, "EURUSD": 600, "USOUSD": 600}

stats = {
    "signals_today":     0,
    "last_scan":         None,
    "start_time":        datetime.now(timezone.utc),
    "last_signal_sent":  None,
    "last_heartbeat":    None,
    "ws_connected":      False,
    "auto_trades_today": 0,
    "daily_losses":      0,
    "kill_switch_active":False,
    "account_balance":   None,
}
announced_sessions = set()
paused_markets     = set()
last_update_id     = 0
last_reset_day     = None

# ─── TELEGRAM ─────────────────────────────────────────────
def send_telegram(text, parse_mode="Markdown"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode
        }, timeout=10)
        if not r.json().get("ok"):
            log.warning(f"Telegram failed: {r.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ─── CANDLE FETCHER ───────────────────────────────────────
def fetch_candles(inst_id, count=50):
    """
    Fetch last `count` completed 1M candles from TwelveData.
    Returns list of dicts: {o, h, l, c, t} sorted oldest→newest.
    """
    inst = INST_BY_ID[inst_id]
    symbol = inst["td_symbol"]
    try:
        url = (
            f"https://api.twelvedata.com/time_series"
            f"?symbol={symbol}&interval=1min&outputsize={count}"
            f"&apikey={TWELVEDATA_API_KEY}"
        )
        r = requests.get(url, timeout=15)
        data = r.json()

        if data.get("status") == "error" or "values" not in data:
            log.warning(f"Candle fetch error {inst_id}: {data.get('message','unknown')}")
            return []

        candles = []
        for v in reversed(data["values"]):  # reverse: API returns newest first
            try:
                o = float(v["open"])
                h = float(v["high"])
                l = float(v["low"])
                c = float(v["close"])

                # NAS100 — TwelveData NDX returns ~20000 range directly, no multiplier needed
                candles.append({"o": o, "h": h, "l": l, "c": c, "t": v["datetime"]})
            except Exception:
                continue

        # Validate price range
        vmin = inst.get("valid_min", 0)
        vmax = inst.get("valid_max", 999999)
        candles = [c for c in candles if vmin <= c["c"] <= vmax]

        if candles:
            latest_price[inst_id] = candles[-1]["c"]
            log.info(f"{inst_id} candles fetched: {len(candles)} | last close: {candles[-1]['c']}")

        return candles

    except Exception as e:
        log.error(f"fetch_candles {inst_id}: {e}")
        return []

def refresh_candles():
    """Refresh candle store for all instruments."""
    for inst in INSTRUMENTS:
        if inst["id"] in paused_markets:
            continue
        candles = fetch_candles(inst["id"], count=50)
        if candles:
            candle_store[inst["id"]] = candles
        time.sleep(0.5)  # avoid rate limiting

# ─── CANDLE ANALYSIS ENGINE ───────────────────────────────
def candle_body(c):
    return abs(c["c"] - c["o"])

def candle_range(c):
    return c["h"] - c["l"]

def is_bullish(c):
    return c["c"] > c["o"]

def is_bearish(c):
    return c["c"] < c["o"]

def get_structure_candles(candles):
    """
    Identify market structure using closed candles.
    Returns: BULLISH / BEARISH / RANGING
    Uses last 20 candles, identifies swing highs and lows.
    """
    if len(candles) < 10:
        return "UNKNOWN"

    recent = candles[-20:]
    highs, lows = [], []

    for i in range(1, len(recent) - 1):
        if recent[i]["h"] > recent[i-1]["h"] and recent[i]["h"] > recent[i+1]["h"]:
            highs.append(recent[i]["h"])
        if recent[i]["l"] < recent[i-1]["l"] and recent[i]["l"] < recent[i+1]["l"]:
            lows.append(recent[i]["l"])

    if len(highs) >= 2 and len(lows) >= 2:
        hh = highs[-1] > highs[-2]
        hl = lows[-1]  > lows[-2]
        lh = highs[-1] < highs[-2]
        ll = lows[-1]  < lows[-2]
        if hh and hl:
            return "BULLISH"
        if lh and ll:
            return "BEARISH"
    return "RANGING"

def detect_liquidity_sweep_candles(candles):
    """
    Detect liquidity sweep on candles:
    - Price wicks below a recent swing low then closes back above = BUY
    - Price wicks above a recent swing high then closes back below = SELL
    Checks last 3 candles against swing points from last 10-20 candles.
    """
    if len(candles) < 15:
        return None

    lookback = candles[-15:-3]
    last3    = candles[-3:]
    signal_candle = candles[-1]  # most recently closed candle

    swing_high = max(c["h"] for c in lookback)
    swing_low  = min(c["l"] for c in lookback)

    # Bearish sweep: wick above swing high, closed back below
    if signal_candle["h"] > swing_high and signal_candle["c"] < swing_high:
        return "SELL"

    # Bullish sweep: wick below swing low, closed back above
    if signal_candle["l"] < swing_low and signal_candle["c"] > swing_low:
        return "BUY"

    return None

def detect_ob(candles, direction):
    """
    Order Block detection on candles.
    Bullish OB: last significant bearish candle before a bullish impulse
    Bearish OB: last significant bullish candle before a bearish impulse
    Returns True if current price is retesting the OB zone.
    """
    if len(candles) < 5:
        return False

    last  = candles[-1]
    prev  = candles[-2]
    prev2 = candles[-3]

    if direction == "BUY":
        # Bullish OB: prev2 was bearish, prev was strong bullish impulse, last retests
        if is_bearish(prev2) and is_bullish(prev):
            ob_top = prev2["o"]
            ob_bot = prev2["c"]
            if ob_bot <= last["c"] <= ob_top:
                return True
    elif direction == "SELL":
        # Bearish OB: prev2 was bullish, prev was strong bearish impulse, last retests
        if is_bullish(prev2) and is_bearish(prev):
            ob_top = prev2["c"]
            ob_bot = prev2["o"]
            if ob_bot <= last["c"] <= ob_top:
                return True
    return False

def detect_fvg(candles, direction):
    """
    Fair Value Gap: gap between candle[i-2] and candle[i] not covered by candle[i-1].
    Checks if current candle is filling a recent FVG.
    """
    if len(candles) < 4:
        return False

    last = candles[-1]
    for i in range(len(candles) - 4, len(candles) - 1):
        c1 = candles[i]
        c3 = candles[i+2] if i+2 < len(candles) else None
        if not c3:
            continue
        if direction == "BUY":
            # Bullish FVG: c1 high < c3 low (gap to the upside)
            if c1["h"] < c3["l"]:
                fvg_bot = c1["h"]
                fvg_top = c3["l"]
                if fvg_bot <= last["c"] <= fvg_top:
                    return True
        elif direction == "SELL":
            # Bearish FVG: c1 low > c3 high (gap to the downside)
            if c1["l"] > c3["h"]:
                fvg_top = c1["l"]
                fvg_bot = c3["h"]
                if fvg_bot <= last["c"] <= fvg_top:
                    return True
    return False

def detect_bos(candles):
    """
    Break of Structure on candles.
    BOS UP: price closes above recent swing high
    BOS DOWN: price closes below recent swing low
    Returns: BUY / SELL / None
    """
    if len(candles) < 10:
        return None

    lookback      = candles[-10:-2]
    signal_candle = candles[-1]

    swing_high = max(c["h"] for c in lookback)
    swing_low  = min(c["l"] for c in lookback)

    if signal_candle["c"] > swing_high:
        return "BUY"
    if signal_candle["c"] < swing_low:
        return "SELL"
    return None

def signal_candle_quality(candle, direction):
    """
    Check the signal candle (last closed candle) quality.
    - Body must be at least 40% of total range (not a doji)
    - Must close in the right direction
    - Wick in signal direction should be small
    """
    body  = candle_body(candle)
    rng   = candle_range(candle)
    if rng == 0:
        return False
    body_pct = body / rng
    if body_pct < 0.35:
        return False  # doji or indecision candle
    if direction == "BUY" and not is_bullish(candle):
        return False
    if direction == "SELL" and not is_bearish(candle):
        return False
    return True

def price_not_extended(candles, direction, sl_dist):
    """
    Check price hasn't already moved more than 1x SL from the setup point.
    Prevents late entries after the move has already happened.
    """
    if len(candles) < 3:
        return True
    last  = candles[-1]
    prev3 = candles[-4] if len(candles) >= 4 else candles[-3]
    move  = abs(last["c"] - prev3["c"])
    if move > sl_dist * 1.2:
        log.info(f"Move already extended: {move:.2f} vs SL {sl_dist}")
        return False
    return True

def analyze_candles(inst_id):
    """
    Main candle-based analysis engine.
    Uses closed 1M candles only — fires signal on candle close.
    Requires multiple confluences to rate STRONG or MID.
    """
    candles = candle_store.get(inst_id, [])
    inst    = INST_BY_ID[inst_id]
    sl_dist = inst["sl_dist"]
    dec     = inst["decimals"]

    if len(candles) < 20:
        return {"signal": "WAIT", "reason": "Building candle history..."}

    last = candles[-1]  # most recently CLOSED candle
    close_price = last["c"]

    # ── Structure ──
    structure = get_structure_candles(candles)

    # ── Core confluences ──
    liq_sweep = detect_liquidity_sweep_candles(candles)
    bos       = detect_bos(candles)

    # Determine candidate direction
    direction = None
    reasons   = []

    if liq_sweep == "BUY" or bos == "BUY":
        direction = "BUY"
        if liq_sweep == "BUY": reasons.append("Liquidity sweep reversal")
        if bos == "BUY":       reasons.append("Break of structure UP")
    elif liq_sweep == "SELL" or bos == "SELL":
        direction = "SELL"
        if liq_sweep == "SELL": reasons.append("Liquidity sweep reversal")
        if bos == "SELL":       reasons.append("Break of structure DOWN")

    if not direction:
        return {"signal": "WAIT", "reason": "No BOS or liquidity sweep on candles"}

    # Conflicting signals
    if liq_sweep and bos and liq_sweep != bos:
        return {"signal": "WAIT", "reason": "Conflicting BOS vs sweep direction"}

    # ── Additional confluences ──
    ob  = detect_ob(candles, direction)
    fvg = detect_fvg(candles, direction)
    struct_aligned = (
        (direction == "BUY"  and structure == "BULLISH") or
        (direction == "SELL" and structure == "BEARISH") or
        structure == "RANGING"
    )
    quality = signal_candle_quality(last, direction)
    not_late = price_not_extended(candles, direction, sl_dist)

    if ob:  reasons.append("Order block retest")
    if fvg: reasons.append("FVG fill")
    if struct_aligned and structure != "RANGING":
        reasons.append(f"{structure} structure")

    # ── Buy-only filter ──
    if inst.get("buy_only") and direction == "SELL":
        return {"signal": "WAIT", "reason": "Buy-only instrument"}

    # ── Signal quality gate — must pass, not scored ──
    if not quality:
        return {"signal": "WAIT", "reason": "Weak signal candle — doji or wrong direction close"}

    if not not_late:
        return {"signal": "WAIT", "reason": "Move already extended — entry too late"}

    # ── Scoring — quality candle is now a gate, not a score point ──
    # BOS is the primary confluence — always scores
    # Extra confluences boost the score
    score = sum([
        liq_sweep is not None,      # Liquidity sweep
        bos is not None,            # Break of structure
        ob,                         # Order block retest
        fvg,                        # Fair value gap fill
        struct_aligned and structure != "RANGING",  # Aligned structure
    ])

    # BOS alone with quality candle = STRONG (direction confirmed)
    # BOS + any extra confluence = STRONG
    # No BOS but liq sweep + extra = STRONG
    if score >= 2:
        rating = "STRONG 🔥"
    elif score >= 1:
        rating = "MID ⚡"
    else:
        return {"signal": "WAIT", "reason": f"Insufficient confluence ({score}/5)"}

    # ── Build result ──
    tp1_mult = 0.8
    tp2_mult = 1.5 if "MID" in rating else 2.2

    if direction == "BUY":
        return {
            "signal":    "BUY",
            "rating":    rating,
            "entry":     round(close_price, dec),
            "sl":        round(close_price - sl_dist, dec),
            "tp1":       round(close_price + sl_dist * tp1_mult, dec),
            "tp2":       round(close_price + sl_dist * tp2_mult, dec),
            "rr":        f"1:{tp1_mult}",
            "reason":    " + ".join(reasons),
            "structure": structure,
            "score":     score,
            "candle":    f"O:{last['o']} H:{last['h']} L:{last['l']} C:{last['c']}",
        }
    else:
        return {
            "signal":    "SELL",
            "rating":    rating,
            "entry":     round(close_price, dec),
            "sl":        round(close_price + sl_dist, dec),
            "tp1":       round(close_price - sl_dist * tp1_mult, dec),
            "tp2":       round(close_price - sl_dist * tp2_mult, dec),
            "rr":        f"1:{tp1_mult}",
            "reason":    " + ".join(reasons),
            "structure": structure,
            "score":     score,
            "candle":    f"O:{last['o']} H:{last['h']} L:{last['l']} C:{last['c']}",
        }

# ─── METAAPI HELPERS ──────────────────────────────────────
def metaapi_headers():
    return {"auth-token": METAAPI_TOKEN, "Content-Type": "application/json"}

def is_london_ny_session():
    h = datetime.now(timezone.utc).hour
    return (8 <= h < 16) or (13 <= h < 21)

def get_metaapi_symbol(inst_id):
    return {
        "XAUUSD": "XAUUSD.s",
        "NAS100": "NAS100.s",
        "EURUSD": "EURUSD.s",
        "USOUSD": "XTIUSD.s",
    }.get(inst_id, inst_id)

def fetch_account_balance():
    if not METAAPI_TOKEN or not METAAPI_ACCOUNT_ID:
        return None
    try:
        url = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/account-information"
        r   = requests.get(url, headers=metaapi_headers(), timeout=10)
        data = r.json()
        balance = data.get("balance") or data.get("equity")
        if balance:
            stats["account_balance"] = float(balance)
            return float(balance)
    except Exception as e:
        log.error(f"Balance fetch error: {e}")
    return None

def calculate_lot_size(sl_dist, inst_id):
    balance = stats.get("account_balance") or fetch_account_balance()
    if not balance:
        return FALLBACK_LOT
    try:
        risk_amount = balance * RISK_PCT
        pv = {"XAUUSD": 100.0, "NAS100": 1.0, "EURUSD": 10.0, "USOUSD": 100.0}.get(inst_id, 100.0)
        raw_lot = risk_amount / (sl_dist * pv)
        lot = round(raw_lot, 2)
        return max(MIN_LOT, min(MAX_LOT, lot))
    except Exception as e:
        log.error(f"Lot calc error: {e}")
        return FALLBACK_LOT

# ─── AUTO-TRADE PLACEMENT ─────────────────────────────────
def place_auto_trade(inst, analysis):
    if not METAAPI_TOKEN or not METAAPI_ACCOUNT_ID:
        return None
    if stats.get("kill_switch_active"):
        return None

    # Max open signals guard — count unique signal groups
    active_groups = set(t.get("signal_group") for t in auto_trades.values() if t.get("signal_group"))
    if len(active_groups) >= MAX_OPEN_POSITIONS:
        send_telegram(
            f"⏸ *Auto-trade skipped — {inst['id']}*\n"
            f"Already have {len(active_groups)} active signal(s).\n"
            f"_Signal valid — place manually if desired._"
        )
        return None

    try:
        direction    = analysis["signal"]
        entry        = analysis["entry"]
        sl           = analysis["sl"]
        sl_dist      = inst["sl_dist"]
        dec          = inst["decimals"]
        mt5_symbol   = get_metaapi_symbol(inst["id"])
        action       = "ORDER_TYPE_BUY" if direction == "BUY" else "ORDER_TYPE_SELL"
        now_str      = datetime.now(timezone.utc).strftime("%H:%M UTC")
        signal_group = f"{inst['id']}_{int(time.time())}"

        # 3 TP levels
        if direction == "BUY":
            tp1 = round(entry + sl_dist * AUTO_TP1_MULT, dec)
            tp2 = round(entry + sl_dist * AUTO_TP2_MULT, dec)
            tp3 = round(entry + sl_dist * AUTO_TP3_MULT, dec)
        else:
            tp1 = round(entry - sl_dist * AUTO_TP1_MULT, dec)
            tp2 = round(entry - sl_dist * AUTO_TP2_MULT, dec)
            tp3 = round(entry - sl_dist * AUTO_TP3_MULT, dec)

        # Equal lot size for all 3
        lot = calculate_lot_size(sl_dist, inst["id"])

        # Place 3 positions
        placed = []
        for tp, label in [(tp1, "TP1"), (tp2, "TP2"), (tp3, "TP3")]:
            try:
                payload = {
                    "symbol":     mt5_symbol,
                    "actionType": action,
                    "volume":     lot,
                    "stopLoss":   sl,
                    "takeProfit": tp,
                    "comment":    f"SMC-{label}"
                }
                url  = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/trade"
                r    = requests.post(url, headers=metaapi_headers(), json=payload, timeout=15)
                data = r.json()
                log.info(f"MetaAPI [{label}]: {data}")

                trade_id = (
                    data.get("orderId") or
                    data.get("positionId") or
                    data.get("tradeExecutionId") or
                    f"{label}_{int(time.time())}"
                )

                auto_trades[trade_id] = {
                    "inst_id":      inst["id"],
                    "label":        inst["label"],
                    "direction":    direction,
                    "entry":        entry,
                    "sl":           sl,
                    "sl_dist":      sl_dist,
                    "tp":           tp,
                    "tp_label":     label,
                    "lots":         lot,
                    "opened_at":    now_str,
                    "status":       "OPEN",
                    "signal_group": signal_group,
                }
                placed.append((trade_id, label, tp))
                log.info(f"Placed {label}: id={trade_id} lot={lot} tp={tp}")

            except Exception as e:
                log.error(f"Order [{label}] error: {e}")

            time.sleep(0.5)

        if not placed:
            return None

        return signal_group, placed, tp1, tp2, tp3, lot

    except Exception as e:
        log.error(f"place_auto_trade error: {e}")
        return None

def close_auto_trade(trade_id, reason="manual"):
    if trade_id not in auto_trades:
        return False
    trade = auto_trades[trade_id]
    try:
        url = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions/{trade_id}/close"
        r = requests.post(url, headers=metaapi_headers(), timeout=15)
        log.info(f"Close {trade_id}: {r.status_code}")

        trade["status"]       = "CLOSED"
        trade["closed_at"]    = datetime.now(timezone.utc).strftime("%H:%M UTC")
        trade["close_reason"] = reason
        auto_trade_history.append(dict(trade))
        if len(auto_trade_history) > 10:
            auto_trade_history.pop(0)
        del auto_trades[trade_id]

        # Kill-switch counter
        if reason in ("sl_hit",):
            stats["daily_losses"] += 1
            log.info(f"Daily losses: {stats['daily_losses']}/{MAX_DAILY_LOSSES}")
            if stats["daily_losses"] >= MAX_DAILY_LOSSES:
                stats["kill_switch_active"] = True
                send_telegram(
                    f"🚨 *DAILY LOSS LIMIT REACHED*\n"
                    f"`{stats['daily_losses']}` losses today.\n"
                    f"🔴 Auto-trading *DISABLED* for rest of today.\n"
                    f"_Resets at 00:00 UTC. Type `auto on` to override._"
                )
        return True
    except Exception as e:
        log.error(f"close_trade error: {e}")
        return False

def move_sl_to_breakeven(trade_id):
    if trade_id not in auto_trades:
        return
    trade = auto_trades[trade_id]
    if trade.get("sl_moved"):
        return
    try:
        url = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions/{trade_id}"
        r = requests.put(url, headers=metaapi_headers(),
                         json={"stopLoss": trade["entry"]}, timeout=15)
        log.info(f"SL→BE {trade_id}: {r.status_code}")
        trade["sl_moved"] = True
        trade["sl"]       = trade["entry"]
    except Exception as e:
        log.error(f"move_sl error: {e}")

# ─── AUTO-TRADE MONITOR ───────────────────────────────────
def monitor_auto_trades(inst_id, current_price):
    if not auto_trades:
        return
    inst = INST_BY_ID[inst_id]
    dec  = inst["decimals"]
    trades_to_close = []

    # Group by signal
    groups = {}
    for tid, trade in auto_trades.items():
        if trade["inst_id"] != inst_id or trade["status"] != "OPEN":
            continue
        sg = trade.get("signal_group", tid)
        groups.setdefault(sg, []).append((tid, trade))

    for sg, group in groups.items():
        for trade_id, trade in group:
            direction = trade["direction"]
            entry     = trade["entry"]
            sl        = trade["sl"]
            tp        = trade["tp"]
            tp_label  = trade.get("tp_label", "TP")
            price     = current_price

            if direction == "BUY":
                if price <= sl:
                    send_telegram(
                        f"🔴 *SL HIT — {trade['inst_id']} ({tp_label})*\n"
                        f"Price `{round(price,dec)}` | SL `{sl}` | Entry `{entry}`"
                    )
                    trades_to_close.append((trade_id, "sl_hit"))
                elif price >= tp:
                    send_telegram(
                        f"✅ *{tp_label} HIT — {trade['inst_id']}*\n"
                        f"Price `{round(price,dec)}` reached `{tp}`\n"
                        f"💼 `{trade['lots']}` lots closed automatically."
                        + ("\n🔒 _Moving remaining SL to breakeven._" if tp_label == "TP1" else "")
                        + ("\n🏆 _Runner target reached!_" if tp_label == "TP3" else "")
                    )
                    trades_to_close.append((trade_id, f"{tp_label.lower()}_hit"))
                    # Move SL to breakeven on TP2 and TP3 after TP1 hits
                    if tp_label == "TP1":
                        for oid, ot in group:
                            if oid != trade_id and ot["status"] == "OPEN":
                                move_sl_to_breakeven(oid)

            elif direction == "SELL":
                if price >= sl:
                    send_telegram(
                        f"🔴 *SL HIT — {trade['inst_id']} ({tp_label})*\n"
                        f"Price `{round(price,dec)}` | SL `{sl}` | Entry `{entry}`"
                    )
                    trades_to_close.append((trade_id, "sl_hit"))
                elif price <= tp:
                    send_telegram(
                        f"✅ *{tp_label} HIT — {trade['inst_id']}*\n"
                        f"Price `{round(price,dec)}` reached `{tp}`\n"
                        f"💼 `{trade['lots']}` lots closed automatically."
                        + ("\n🔒 _Moving remaining SL to breakeven._" if tp_label == "TP1" else "")
                        + ("\n🏆 _Runner target reached!_" if tp_label == "TP3" else "")
                    )
                    trades_to_close.append((trade_id, f"{tp_label.lower()}_hit"))
                    if tp_label == "TP1":
                        for oid, ot in group:
                            if oid != trade_id and ot["status"] == "OPEN":
                                move_sl_to_breakeven(oid)

    for trade_id, reason in trades_to_close:
        close_auto_trade(trade_id, reason)

# ─── SIGNAL FORMATTER ─────────────────────────────────────
def get_lot_recommendation(rating, inst):
    if "STRONG" in rating: return inst.get("lot_strong", 0.03)
    elif "MID" in rating:  return inst.get("lot_mid", 0.02)
    return 0.01

def estimate_profit(inst_id, entry, tp1, tp2, sl, lots):
    try:
        d1 = abs(float(tp1) - float(entry))
        d2 = abs(float(tp2) - float(entry))
        ds = abs(float(sl)  - float(entry))
        pv = {"XAUUSD": 100, "NAS100": 1, "USOUSD": 1000, "EURUSD": 100000}.get(inst_id, 100)
        if inst_id == "EURUSD":
            return round(pv*lots*d1,2), round(pv*lots*d2,2), round(pv*lots*ds,2)
        return round(d1*pv*lots,2), round(d2*pv*lots,2), round(ds*pv*lots,2)
    except:
        return 0.0, 0.0, 0.0

def format_signal(inst, a):
    now   = datetime.now(timezone.utc).strftime("%H:%M UTC")
    emoji = "🟢" if a["signal"] == "BUY" else "🔴"
    rating = a.get("rating", "MID ⚡")
    lots   = get_lot_recommendation(rating, inst)
    p1, p2, ls = estimate_profit(inst["id"], a["entry"], a["tp1"], a["tp2"], a["sl"], lots)
    score_str = f" ({a.get('score','?')}/6 confluence)" if a.get("score") else ""
    return (
        f"{emoji} *{a['signal']} — {inst['id']}* ({inst['label']})\n"
        f"⭐ Rating: *{rating}*{score_str}\n"
        f"⏰ `{now}` | Candle close signal\n"
        f"━━━━━━━━━━━━━━\n"
        f"📍 Entry:  *{a['entry']}*\n"
        f"🛑 SL:     `{a['sl']}`\n"
        f"🎯 TP1:    `{a['tp1']}`\n"
        f"🎯 TP2:    `{a['tp2']}`\n"
        f"📊 RR:     `{a['rr']}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"💼 Lot Size:  *{lots}*\n"
        f"💰 Est. Profit TP1: *~${p1}*\n"
        f"💰 Est. Profit TP2: *~${p2}*\n"
        f"🔻 Est. Loss if SL: *~-${ls}*\n"
        f"━━━━━━━━━━━━━━\n"
        f"🧠 _{a['reason']}_\n"
        f"📈 Structure: `{a.get('structure','N/A')}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"Copy TP1: `{a['tp1']}`\n"
        f"Copy TP2: `{a['tp2']}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"⚠️ _Signal fires on candle close — entry at close price_"
    )

# ─── AUTO POSITIONS FORMATTER ─────────────────────────────
def format_auto_positions():
    if not auto_trades:
        return "📭 *No open auto-trades right now.*"

    groups = {}
    for tid, t in auto_trades.items():
        sg = t.get("signal_group", tid)
        groups.setdefault(sg, []).append((tid, t))

    lines = ["🤖 *Open Auto-Trade Positions*\n"]
    for sg, group in groups.items():
        _, sample = group[0]
        price     = latest_price.get(sample["inst_id"])
        price_str = str(round(price, 2)) if price else "N/A"
        emoji     = "🟢" if sample["direction"] == "BUY" else "🔴"
        lines.append(
            f"{emoji} *{sample['inst_id']}* {sample['direction']}\n"
            f"📍 Entry `{sample['entry']}` | Now `{price_str}`\n"
            f"🛑 SL: `{sample['sl']}` | Opened: `{sample['opened_at']}`"
        )
        for tid, t in sorted(group, key=lambda x: x[1].get("tp_label", "")):
            sl_str = "🔒 BE" if t.get("sl_moved") else f"`{t['sl']}`"
            lines.append(
                f"  • *{t.get('tp_label','TP')}*: `{t['tp']}` "
                f"| `{t['lots']}` lots | SL {sl_str}"
            )
        lines.append("━━━━━━━━━━━━━━")
    return "\n".join(lines)

def format_auto_history():
    if not auto_trade_history:
        return "📭 *No closed auto-trades yet.*"
    lines = ["📜 *Last Closed Auto-Trades*\n"]
    reason_map = {
        "tp3_hit": "🏆 TP3 Hit", "tp2_hit": "✅ TP2 Hit",
        "tp1_hit": "✅ TP1 Hit", "sl_hit":  "🔴 SL Hit",
        "manual":  "🖐 Manual",
    }
    for t in reversed(auto_trade_history[-5:]):
        emoji   = "🟢" if t["direction"] == "BUY" else "🔴"
        outcome = reason_map.get(t.get("close_reason",""), t.get("close_reason","Closed"))
        lines.append(
            f"{emoji} *{t['inst_id']}* {t['direction']} — {outcome}\n"
            f"Entry `{t['entry']}` | {t.get('tp_label','')} `{t.get('tp','')}`\n"
            f"Opened `{t['opened_at']}` | Closed `{t.get('closed_at','N/A')}`\n"
            f"━━━━━━━━━━━━━━"
        )
    return "\n".join(lines)

# ─── MANUAL TP/SL MONITOR ─────────────────────────────────
def check_tp_sl(inst_id, current_price):
    inst = INST_BY_ID[inst_id]
    if inst_id not in open_signals:
        return
    sig = open_signals[inst_id]
    dec = inst["decimals"]
    p   = current_price
    d   = sig["direction"]

    if d == "BUY":
        if not sig["tp1_hit"] and p >= sig["tp1"]:
            sig["tp1_hit"] = True
            send_telegram(
                f"*TP1 HIT — {inst_id}*\nPrice `{round(p,dec)}` | TP1 `{sig['tp1']}`\n"
                f"_Move SL to breakeven. TP2: `{sig['tp2']}`_"
            )
        elif sig["tp1_hit"] and p >= sig["tp2"]:
            send_telegram(f"*TP2 HIT — {inst_id}*\nPrice `{round(p,dec)}` | Full target! Close trade.")
            del open_signals[inst_id]
        elif p <= sig["sl"]:
            send_telegram(f"*SL HIT — {inst_id}*\nPrice `{round(p,dec)}` | SL `{sig['sl']}`")
            del open_signals[inst_id]
    elif d == "SELL":
        if not sig["tp1_hit"] and p <= sig["tp1"]:
            sig["tp1_hit"] = True
            send_telegram(
                f"*TP1 HIT — {inst_id}*\nPrice `{round(p,dec)}` | TP1 `{sig['tp1']}`\n"
                f"_Move SL to breakeven. TP2: `{sig['tp2']}`_"
            )
        elif sig["tp1_hit"] and p <= sig["tp2"]:
            send_telegram(f"*TP2 HIT — {inst_id}*\nPrice `{round(p,dec)}` | Full target! Close trade.")
            del open_signals[inst_id]
        elif p >= sig["sl"]:
            send_telegram(f"*SL HIT — {inst_id}*\nPrice `{round(p,dec)}` | SL `{sig['sl']}`")
            del open_signals[inst_id]

# ─── SESSION HELPERS ──────────────────────────────────────
def get_active_session(now):
    h = now.hour
    s = []
    if 7  <= h < 16: s.append("London 🇬🇧")
    if 12 <= h < 21: s.append("New York 🇺🇸")
    if 0  <= h < 9:  s.append("Tokyo 🇯🇵")
    if h >= 22 or h < 7: s.append("Sydney 🇦🇺")
    return " + ".join(s) if s else "Off-hours"

SESSION_MSGS = {
    "London_open":   ("🇬🇧 *London Session Open* — 08:00 UTC\nPrime window. Auto-trading ACTIVE.", 8),
    "NewYork_open":  ("🇺🇸 *New York Session Open* — 13:00 UTC\nHigh volatility. Auto-trading ACTIVE.", 13),
    "London_close":  ("🇬🇧 *London Closing* — 16:00 UTC\nLiquidity dropping.", 16),
    "NewYork_close": ("🇺🇸 *New York Closing* — 21:00 UTC\nAuto-trading PAUSED until next session.", 21),
    "Asian_open":    ("🌏 *Asian Session* — 00:00 UTC\nLower liquidity. Manual trades only.", 0),
}

def check_session_announcements():
    now = datetime.now(timezone.utc)
    if now.minute > 2:
        return
    for key, (msg_text, h) in SESSION_MSGS.items():
        dk = f"{key}_{now.date()}"
        if now.hour == h and dk not in announced_sessions:
            send_telegram(msg_text)
            announced_sessions.add(dk)

def check_heartbeat():
    now = datetime.now(timezone.utc)
    ref = stats["last_signal_sent"] or stats["start_time"]
    if stats["last_heartbeat"] and (now - stats["last_heartbeat"]).total_seconds() < 1200:
        return
    if (now - ref).total_seconds() < 1200:
        return
    ps = ""
    for i in INSTRUMENTS:
        p = latest_price[i["id"]]
        if p:
            ps += f"`{i['id']}: {round(p, i['decimals'])}` "
    send_telegram(
        f"💓 *Heartbeat* `{now.strftime('%H:%M UTC')}`\n"
        f"No signals in 20 mins.\n"
        f"Session: `{get_active_session(now)}`\n{ps}"
    )
    stats["last_heartbeat"] = now

def reset_daily_stats():
    global last_reset_day
    today = datetime.now(timezone.utc).date()
    if last_reset_day != today:
        stats["signals_today"]       = 0
        stats["auto_trades_today"]   = 0
        stats["daily_losses"]        = 0
        stats["kill_switch_active"]  = False
        last_reset_day = today
        announced_sessions.clear()
        log.info("Daily stats reset")

def send_status():
    now = datetime.now(timezone.utc)
    up  = now - stats["start_time"]
    h, rem = divmod(int(up.total_seconds()), 3600)
    m = rem // 60
    ls   = stats["last_scan"].strftime("%H:%M:%S UTC") if stats["last_scan"] else "N/A"
    lsig = stats["last_signal_sent"].strftime("%H:%M UTC") if stats["last_signal_sent"] else "None"
    auto_str = "✅ ON" if AUTO_TRADE_ENABLED else "⏸ OFF"
    bal_str  = f"${stats['account_balance']:.2f}" if stats["account_balance"] else "Unknown"
    send_telegram(
        f"✅ *Bot Online — Candle Engine v5*\n"
        f"🕐 `{now.strftime('%H:%M:%S UTC')}` | Uptime `{h}h {m}m`\n"
        f"━━━━━━━━━━━━━━\n"
        f"🤖 Auto-Trade: {auto_str} | Balance: `{bal_str}`\n"
        f"📊 Signals today: `{stats['signals_today']}`\n"
        f"🤖 Auto-trades today: `{stats['auto_trades_today']}`\n"
        f"❌ Losses today: `{stats['daily_losses']}/{MAX_DAILY_LOSSES}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"🌍 Session: `{get_active_session(now)}`\n"
        f"🕵️ Last scan: `{ls}`\n"
        f"⏰ Last signal: `{lsig}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"_1M candle engine — signals fire on candle close_"
    )

# ─── TELEGRAM COMMANDS ────────────────────────────────────
def check_telegram_commands():
    global last_update_id, AUTO_TRADE_ENABLED
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?timeout=1&offset={last_update_id+1}&limit=10"
        r = requests.get(url, timeout=6)
        for u in r.json().get("result", []):
            last_update_id = u.get("update_id", last_update_id)
            msg = u.get("message", {}).get("text", "").strip().lower()

            if msg in ("auto positions", "auto position"):
                send_telegram(format_auto_positions())
            elif msg == "auto history":
                send_telegram(format_auto_history())
            elif msg == "auto off":
                AUTO_TRADE_ENABLED = False
                send_telegram("⏸ *Auto-trading DISABLED.*")
            elif msg == "auto on":
                AUTO_TRADE_ENABLED = True
                send_telegram("▶️ *Auto-trading ENABLED.*")
            elif msg == "auto status":
                session_active = is_london_ny_session()
                kill_str   = "🚨 ACTIVE" if stats["kill_switch_active"] else "✅ Clear"
                bal_str    = f"${stats['account_balance']:.2f}" if stats["account_balance"] else "Fetching..."
                lot        = calculate_lot_size(3.5, "XAUUSD")
                send_telegram(
                    f"🤖 *Auto-Trade Status*\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"Switch: {'✅ ENABLED' if AUTO_TRADE_ENABLED else '⏸ DISABLED'}\n"
                    f"Session: {'✅ London/NY Active' if session_active else '⏳ Outside hours'}\n"
                    f"Kill-switch: {kill_str}\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"💰 Balance: `{bal_str}`\n"
                    f"💼 Lot size: `{lot}` _(1% risk dynamic)_\n"
                    f"🔒 Max signals: `{MAX_OPEN_POSITIONS}`\n"
                    f"🛑 Max losses: `{MAX_DAILY_LOSSES}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"📈 Open trades: `{len(auto_trades)}`\n"
                    f"📨 Trades today: `{stats['auto_trades_today']}`\n"
                    f"❌ Losses today: `{stats['daily_losses']}/{MAX_DAILY_LOSSES}`"
                )
            elif msg in ("/status", "status"):
                send_status()
            elif msg in ("/help", "help", "prompts"):
                send_telegram(
                    "📋 *SMC Bot Commands*\n\n"
                    "*Signals*\n"
                    "`status` — Bot status\n"
                    "`markets` — Live prices\n"
                    "`pause XAUUSD` / `pause all`\n"
                    "`resume XAUUSD` / `resume all`\n"
                    "`xauusd` `nas100` `eurusd` `usousd` — Live price\n\n"
                    "*Auto-Trading*\n"
                    "`auto positions` — Open trades\n"
                    "`auto history` — Last 5 closed\n"
                    "`auto status` — Full status\n"
                    "`auto on` / `auto off`\n\n"
                    "_Engine: 1M candle close signals_"
                )
            elif msg.startswith("pause"):
                t = msg.split()[1].upper() if len(msg.split()) > 1 else "ALL"
                if t == "ALL":
                    for i in INSTRUMENTS: paused_markets.add(i["id"])
                    send_telegram("⏸ *All markets paused.*")
                elif t in INST_BY_ID:
                    paused_markets.add(t)
                    send_telegram(f"⏸ *{t}* paused.")
            elif msg.startswith("resume"):
                t = msg.split()[1].upper() if len(msg.split()) > 1 else "ALL"
                if t == "ALL":
                    paused_markets.clear()
                    send_telegram("▶️ *All markets resumed.*")
                else:
                    paused_markets.discard(t)
                    send_telegram(f"▶️ *{t}* resumed.")
            elif msg == "/markets" or msg == "markets":
                lines = []
                for i in INSTRUMENTS:
                    st = "⏸" if i["id"] in paused_markets else "✅"
                    pr = latest_price[i["id"]]
                    pr = round(pr, i["decimals"]) if pr else "N/A"
                    lines.append(f"{st} `{i['id']}` @ `{pr}`")
                send_telegram("📊 *Markets*\n" + "\n".join(lines))
            else:
                for i in INSTRUMENTS:
                    if msg == i["id"].lower():
                        p = latest_price[i["id"]]
                        if p:
                            dec = i["decimals"]
                            sp  = i["sl_dist"] * 0.1
                            send_telegram(
                                f"*{i['id']}* ({i['label']})\n"
                                f"BUY: `{round(p+sp,dec)}` | SELL: `{round(p-sp,dec)}`"
                            )
                        else:
                            send_telegram(f"No price data for `{i['id']}` yet.")
                        break
    except Exception as e:
        log.warning(f"Command error: {e}")

# ─── PRICE MONITOR (WebSocket for live TP/SL checks) ──────
SYMBOL_TO_ID = {i["symbol"]: i["id"] for i in INSTRUMENTS}

def on_ws_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("event") == "price":
            symbol = data.get("symbol")
            price  = float(data.get("price", 0))
            if symbol in SYMBOL_TO_ID and price > 0:
                inst_id = SYMBOL_TO_ID[symbol]
                inst    = INST_BY_ID[inst_id]
                vmin    = inst.get("valid_min", 0)
                vmax    = inst.get("valid_max", 999999)
                if vmin <= price <= vmax:
                    latest_price[inst_id] = price
                    # Monitor auto-trades on every tick
                    monitor_auto_trades(inst_id, price)
                    check_tp_sl(inst_id, price)
    except Exception as e:
        log.error(f"WS message error: {e}")

def on_ws_open(ws):
    stats["ws_connected"] = True
    symbols = ",".join(i["symbol"] for i in INSTRUMENTS)
    ws.send(json.dumps({"action": "subscribe", "params": {"symbols": symbols}}))
    log.info(f"WS subscribed: {symbols}")

def on_ws_error(ws, error):
    stats["ws_connected"] = False
    log.error(f"WS error: {error}")

def on_ws_close(ws, code, msg):
    stats["ws_connected"] = False
    log.warning(f"WS closed: {code}")

def run_websocket():
    if websocket is None:
        return
    url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_API_KEY}"
    while True:
        try:
            ws = websocket.WebSocketApp(
                url, on_open=on_ws_open, on_message=on_ws_message,
                on_error=on_ws_error, on_close=on_ws_close
            )
            ws.run_forever(ping_interval=10, ping_timeout=5)
        except Exception as e:
            log.error(f"WS run error: {e}")
        time.sleep(5)

# ─── MAIN ANALYSIS LOOP ───────────────────────────────────
def analysis_loop():
    while True:
        try:
            stats["last_scan"] = datetime.now(timezone.utc)

            # Fetch fresh candles for all instruments
            refresh_candles()

            for inst in INSTRUMENTS:
                if inst["id"] in paused_markets:
                    continue

                inst_id = inst["id"]
                analysis = analyze_candles(inst_id)
                sig      = analysis["signal"]
                prev_sig = last_signals[inst_id]

                log.info(
                    f"{inst_id} | close={latest_price[inst_id]} | "
                    f"{sig} | {analysis.get('reason','')}"
                )

                if sig != "WAIT" and sig != prev_sig:
                    now_t  = datetime.now(timezone.utc)
                    last_t = last_signal_time[inst_id]
                    cd     = COOLDOWNS.get(inst_id, 600)
                    in_cd  = last_t and (now_t - last_t).total_seconds() < cd

                    if in_cd:
                        rem = int((cd - (now_t - last_t).total_seconds()) / 60)
                        log.info(f"{inst_id} cooldown {rem}m left")
                        last_signals[inst_id] = sig
                    else:
                        last_signals[inst_id]      = sig
                        last_signal_time[inst_id]  = now_t
                        rating = analysis.get("rating", "")

                        # ── AUTO-TRADE ──
                        auto_placed = False
                        if (
                            AUTO_TRADE_ENABLED
                            and inst_id == AUTO_TRADE_SYMBOL
                            and "STRONG" in rating
                            and is_london_ny_session()
                            and METAAPI_TOKEN
                            and METAAPI_ACCOUNT_ID
                        ):
                            result = place_auto_trade(inst, analysis)
                            if result:
                                auto_placed = True
                                stats["auto_trades_today"] += 1
                                sg, placed, tp1, tp2, tp3, lot = result
                                emoji = "🟢" if sig == "BUY" else "🔴"
                                send_telegram(
                                    f"🤖 *{len(placed)}/3 POSITIONS PLACED — {inst_id}*\n"
                                    f"{emoji} *{sig}* | ⭐ STRONG 🔥\n"
                                    f"━━━━━━━━━━━━━━\n"
                                    f"📍 Entry: `{analysis['entry']}`\n"
                                    f"🛑 SL:    `{analysis['sl']}`\n"
                                    f"🎯 TP1:   `{tp1}` | `{lot}` lots\n"
                                    f"🎯 TP2:   `{tp2}` | `{lot}` lots\n"
                                    f"🎯 TP3:   `{tp3}` | `{lot}` lots\n"
                                    f"━━━━━━━━━━━━━━\n"
                                    f"🔒 SL → breakeven after TP1\n"
                                    f"_Each position closes at its own TP_\n"
                                    f"_Type `auto positions` to monitor_"
                                )
                            else:
                                send_telegram(
                                    f"⚠️ *AUTO-TRADE FAILED — {inst_id}*\n"
                                    f"MetaAPI error. Place manually:\n"
                                    f"Entry `{analysis['entry']}` | SL `{analysis['sl']}`"
                                )

                        # Always send signal alert
                        send_telegram(format_signal(inst, analysis))
                        stats["signals_today"]   += 1
                        stats["last_signal_sent"] = now_t
                        stats["last_heartbeat"]   = now_t

                        # Store for manual monitoring
                        open_signals[inst_id] = {
                            "direction": sig,
                            "entry":     analysis["entry"],
                            "sl":        analysis["sl"],
                            "tp1":       analysis["tp1"],
                            "tp2":       analysis["tp2"],
                            "tp1_hit":   False,
                        }

                elif sig == "WAIT" and prev_sig in ("BUY", "SELL"):
                    last_signals[inst_id] = "WAIT"

            reset_daily_stats()
            check_session_announcements()
            check_heartbeat()
            check_telegram_commands()

        except Exception as e:
            log.error(f"Analysis loop error: {e}")

        time.sleep(ANALYZE_INTERVAL)

# ─── MAIN ─────────────────────────────────────────────────
def main():
    log.info("SMC Bot v5 — Candle Engine Starting")

    # Fetch balance on startup
    if METAAPI_TOKEN and METAAPI_ACCOUNT_ID:
        balance = fetch_account_balance()
        if balance:
            lot = calculate_lot_size(3.5, "XAUUSD")
            log.info(f"Balance: ${balance:.2f} | Lot: {lot}")

    bal_str  = f"${stats['account_balance']:.2f}" if stats["account_balance"] else "Unknown"
    lot_str  = str(calculate_lot_size(3.5, "XAUUSD")) if stats["account_balance"] else str(FALLBACK_LOT)
    auto_str = "✅ ENABLED" if (AUTO_TRADE_ENABLED and METAAPI_TOKEN) else "⚠️ DISABLED"

    send_telegram(
        f"✅ *SMC Signal Bot v5 — Candle Engine*\n"
        f"📊 Monitoring: `XAUUSD | NAS100 | EURUSD | USOUSD`\n"
        f"📡 Price feed: `WebSocket (real-time ticks)`\n"
        f"📊 Analysis: `1M closed candles every 60s`\n"
        f"🤖 Auto-Trade: {auto_str}\n"
        f"━━━━━━━━━━━━━━\n"
        f"🛡️ *Safety Features*\n"
        f"💰 Balance: `{bal_str}` | Lot: `{lot_str}` _(1% risk)_\n"
        f"🔒 Max signals: `{MAX_OPEN_POSITIONS}`\n"
        f"🛑 Kill-switch: `{MAX_DAILY_LOSSES} losses = stop`\n"
        f"━━━━━━━━━━━━━━\n"
        f"_Signals fire on candle CLOSE — no more late entries_\n"
        f"_STRONG XAUUSD → auto-placed London/NY only_\n"
        f"Type `help` for commands."
    )

    # Start WebSocket for live price feed (TP/SL monitoring)
    if USE_WEBSOCKET and websocket:
        t = threading.Thread(target=run_websocket, daemon=True)
        t.start()
        log.info("WebSocket started")

    time.sleep(5)
    analysis_loop()

if __name__ == "__main__":
    main()
