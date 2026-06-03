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
ANALYZE_INTERVAL   = int(os.environ.get("ANALYZE_INTERVAL", "5"))
USE_WEBSOCKET      = os.environ.get("USE_WEBSOCKET", "true").lower() == "true"
METAAPI_TOKEN      = os.environ.get("METAAPI_TOKEN", "")
METAAPI_ACCOUNT_ID = os.environ.get("METAAPI_ACCOUNT_ID", "")

# ─── AUTO-TRADE CONFIG ────────────────────────────────────
AUTO_TRADE_ENABLED  = True          # Master switch
AUTO_TRADE_SYMBOL   = "XAUUSD"      # Only auto-trade Gold for now
AUTO_TRADE_SESSIONS = [(8, 16), (13, 21)]  # London 08-16 UTC, NY 13-21 UTC

# TP multipliers for 3-target structure
AUTO_TP1_MULT = 0.8    # Very close — easy first target
AUTO_TP2_MULT = 1.5    # Medium target
AUTO_TP3_MULT = 2.5    # Runner

# Safety close: if price gets within this % of SL, close early
SAFETY_CLOSE_PCT = 0.20   # Close if within 20% of SL distance

# ─── SAFETY FEATURES ──────────────────────────────────────
# 1. Dynamic lot sizing — 1% account risk per trade
RISK_PCT           = 0.01   # Risk 1% of account balance per trade
FALLBACK_LOT       = 0.02   # Used if balance fetch fails
MIN_LOT            = 0.01   # Minimum allowed lot
MAX_LOT            = 0.10   # Hard cap — never exceed this

# 2. Daily loss kill-switch
MAX_DAILY_LOSSES   = 3      # Disable auto-trading after 3 losses today

# 3. Max simultaneous open positions
MAX_OPEN_POSITIONS = 1      # Only 1 auto-trade open at a time

# MetaAPI base URL
META_API_URL = "https://mt-client-api-v1.london.agiliumtrade.ai"

# ─── INSTRUMENTS ──────────────────────────────────────────
INSTRUMENTS = [
    {"id": "XAUUSD", "label": "Gold",    "symbol": "XAU/USD",  "decimals": 2,  "sl_dist": 3.5,    "priority": 1, "ws": True,
     "lot_strong": 0.05, "lot_mid": 0.02, "valid_min": 1000,  "valid_max": 10000},
    {"id": "NAS100", "label": "Nasdaq",  "symbol": "US100",    "decimals": 1,  "sl_dist": 20.0,   "priority": 2, "ws": False,
     "lot_strong": 0.2,  "lot_mid": 0.1,  "valid_min": 10000, "valid_max": 40000, "multiplier": 1000},
    {"id": "EURUSD", "label": "EUR/USD", "symbol": "EUR/USD",  "decimals": 5,  "sl_dist": 0.0015, "priority": 3, "ws": True,
     "lot_strong": 0.03, "lot_mid": 0.02, "valid_min": 0.5,   "valid_max": 2.0},
    {"id": "USOUSD", "label": "WTI Oil", "symbol": "WTI/USD",  "decimals": 2,  "sl_dist": 0.8,    "priority": 4, "ws": False,
     "lot_strong": 0.03, "lot_mid": 0.02, "valid_min": 30,    "valid_max": 130, "buy_only": True},
]
SYMBOL_TO_ID = {i["symbol"]: i["id"] for i in INSTRUMENTS}

# ─── STATE ────────────────────────────────────────────────
price_history    = {i["id"]: [] for i in INSTRUMENTS}
latest_price     = {i["id"]: None for i in INSTRUMENTS}
last_signals     = {i["id"]: None for i in INSTRUMENTS}
last_signal_time = {i["id"]: None for i in INSTRUMENTS}
open_signals     = {}   # manual TP/SL tracking
auto_trades      = {}   # active auto-trades {trade_id: {...}}
auto_trade_history = [] # closed auto-trades (last 10)
feed_alerted     = {}

COOLDOWNS = {
    "XAUUSD": 420,
    "NAS100": 600,
    "EURUSD": 600,
    "USOUSD": 600,
}
stats = {
    "signals_today": 0,
    "last_scan": None,
    "start_time": datetime.now(timezone.utc),
    "last_signal_sent": None,
    "last_heartbeat": None,
    "ws_connected": False,
    "auto_trades_today": 0,
    "daily_losses": 0,          # Safety feature 1: loss counter
    "kill_switch_active": False, # Safety feature 1: daily kill-switch
    "account_balance": None,     # Safety feature 1: dynamic lot sizing
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
            "chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": parse_mode
        }, timeout=10)
        if not r.json().get("ok"):
            log.warning(f"Telegram failed: {r.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

# ─── METAAPI HELPERS ──────────────────────────────────────
def metaapi_headers():
    return {
        "auth-token": METAAPI_TOKEN,
        "Content-Type": "application/json"
    }

def is_london_ny_session():
    """Check if current UTC time is in London or NY session."""
    h = datetime.now(timezone.utc).hour
    in_london = 8 <= h < 16
    in_ny     = 13 <= h < 21
    return in_london or in_ny

def get_metaapi_symbol(inst_id):
    """Map our instrument ID to MetaAPI/MT5 symbol name."""
    mapping = {
        "XAUUSD": "XAUUSD",
        "NAS100": "NAS100",
        "EURUSD": "EURUSD",
        "USOUSD": "XTIUSD",
    }
    return mapping.get(inst_id, inst_id)

# ─── SAFETY FEATURE 1: DYNAMIC LOT SIZING ────────────────
def fetch_account_balance():
    """Fetch live account balance from MetaAPI."""
    if not METAAPI_TOKEN or not METAAPI_ACCOUNT_ID:
        return None
    try:
        url = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/account-information"
        r = requests.get(url, headers=metaapi_headers(), timeout=10)
        data = r.json()
        balance = data.get("balance") or data.get("equity")
        if balance:
            stats["account_balance"] = float(balance)
            log.info(f"Account balance fetched: ${balance}")
            return float(balance)
    except Exception as e:
        log.error(f"Balance fetch error: {e}")
    return None

def calculate_lot_size(sl_dist_points, inst_id):
    """
    Dynamic lot sizing: risk 1% of account balance per trade.
    Formula: lot = (balance * RISK_PCT) / (sl_dist_points * pip_value)
    """
    balance = stats.get("account_balance") or fetch_account_balance()
    if not balance:
        log.warning("Balance unavailable — using fallback lot size")
        return FALLBACK_LOT
    try:
        risk_amount = balance * RISK_PCT  # e.g. $529 * 0.01 = $5.29

        # Pip/point value per 0.01 lot for each instrument
        if inst_id == "XAUUSD":
            # Gold: $1 per 1.0 point per 0.01 lot
            point_value_per_001 = 1.0
        elif inst_id == "NAS100":
            # NAS100: $1 per 1.0 point per 1.0 lot → $0.01 per 0.01 lot
            point_value_per_001 = 0.01
        elif inst_id == "EURUSD":
            # EUR/USD: ~$1 per pip per 0.1 lot → $0.1 per 0.01 lot
            point_value_per_001 = 0.1
        else:
            point_value_per_001 = 1.0

        # lot = risk_amount / (sl_points * point_value_per_lot)
        # point_value_per_lot = point_value_per_001 * 100
        point_value_per_lot = point_value_per_001 * 100
        raw_lot = risk_amount / (sl_dist_points * point_value_per_lot)

        # Round to 2 decimal places, clamp between MIN and MAX
        lot = round(raw_lot, 2)
        lot = max(MIN_LOT, min(MAX_LOT, lot))
        log.info(f"Dynamic lot: balance=${balance:.2f} risk=${risk_amount:.2f} sl={sl_dist_points} → lot={lot}")
        return lot
    except Exception as e:
        log.error(f"Lot calculation error: {e}")
        return FALLBACK_LOT

def place_auto_trade(inst, analysis):
    """Place a trade via MetaAPI. Returns trade_id or None."""
    if not METAAPI_TOKEN or not METAAPI_ACCOUNT_ID:
        log.warning("MetaAPI credentials missing — cannot auto-trade")
        return None

    # ── SAFETY FEATURE 2: Max open positions guard ──
    open_count = len(auto_trades)
    if open_count >= MAX_OPEN_POSITIONS:
        log.info(f"Max positions ({MAX_OPEN_POSITIONS}) reached — skipping auto-trade")
        send_telegram(
            f"⏸ *Auto-trade skipped — {inst['id']}*\n"
            f"Already have {open_count} open position(s).\n"
            f"Max allowed: `{MAX_OPEN_POSITIONS}`\n"
            f"_Signal still valid — place manually if desired._"
        )
        return None

    # ── SAFETY FEATURE 1: Kill-switch check ──
    if stats.get("kill_switch_active"):
        log.info("Kill-switch active — auto-trading disabled for today")
        return None

    try:
        direction = analysis["signal"]
        entry     = analysis["entry"]
        sl        = analysis["sl"]
        sl_dist   = inst["sl_dist"]
        dec       = inst["decimals"]

        # ── SAFETY FEATURE 1: Dynamic lot sizing ──
        lot = calculate_lot_size(sl_dist, inst["id"])

        # 3-target TP structure
        if direction == "BUY":
            tp1 = round(entry + sl_dist * AUTO_TP1_MULT, dec)
            tp2 = round(entry + sl_dist * AUTO_TP2_MULT, dec)
            tp3 = round(entry + sl_dist * AUTO_TP3_MULT, dec)
            action = "ORDER_TYPE_BUY"
        else:
            tp1 = round(entry - sl_dist * AUTO_TP1_MULT, dec)
            tp2 = round(entry - sl_dist * AUTO_TP2_MULT, dec)
            tp3 = round(entry - sl_dist * AUTO_TP3_MULT, dec)
            action = "ORDER_TYPE_SELL"

        mt5_symbol = get_metaapi_symbol(inst["id"])

        payload = {
            "symbol":     mt5_symbol,
            "actionType": action,
            "volume":     lot,
            "stopLoss":   sl,
            "takeProfit": tp1,
            "comment":    "SMC-Bot-Auto"
        }

        url = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/trade"
        r = requests.post(url, headers=metaapi_headers(), json=payload, timeout=15)
        data = r.json()

        log.info(f"MetaAPI response: {data}")

        # Extract trade/order ID
        trade_id = (
            data.get("orderId") or
            data.get("positionId") or
            data.get("tradeExecutionId") or
            str(int(time.time()))
        )

        # Store auto-trade state
        auto_trades[trade_id] = {
            "inst_id":   inst["id"],
            "label":     inst["label"],
            "direction": direction,
            "entry":     entry,
            "sl":        sl,
            "sl_dist":   sl_dist,
            "tp1":       tp1,
            "tp2":       tp2,
            "tp3":       tp3,
            "tp1_hit":   False,
            "tp2_hit":   False,
            "sl_moved":  False,
            "lots":      lot,
            "opened_at": datetime.now(timezone.utc).strftime("%H:%M UTC"),
            "status":    "OPEN",
        }

        return trade_id

    except Exception as e:
        log.error(f"MetaAPI place_trade error: {e}")
        return None

def close_auto_trade(trade_id, reason="manual"):
    """Close a trade via MetaAPI."""
    if trade_id not in auto_trades:
        return False
    trade = auto_trades[trade_id]
    try:
        if not METAAPI_TOKEN or not METAAPI_ACCOUNT_ID:
            return False

        url = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions/{trade_id}/close"
        r = requests.post(url, headers=metaapi_headers(), timeout=15)
        log.info(f"Close trade {trade_id}: {r.status_code} {r.text}")

        # Move to history
        trade["status"]       = "CLOSED"
        trade["closed_at"]    = datetime.now(timezone.utc).strftime("%H:%M UTC")
        trade["close_reason"] = reason
        auto_trade_history.append(dict(trade))
        if len(auto_trade_history) > 10:
            auto_trade_history.pop(0)
        del auto_trades[trade_id]

        # ── SAFETY FEATURE 1: Daily loss kill-switch ──
        if reason in ("sl_hit", "safety_close"):
            stats["daily_losses"] += 1
            log.info(f"Daily losses: {stats['daily_losses']}/{MAX_DAILY_LOSSES}")
            if stats["daily_losses"] >= MAX_DAILY_LOSSES:
                stats["kill_switch_active"] = True
                send_telegram(
                    f"🚨 *DAILY LOSS LIMIT REACHED*\n"
                    f"Bot has taken `{stats['daily_losses']}` losses today.\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"🔴 Auto-trading *DISABLED* for the rest of today.\n"
                    f"Signals will still fire for manual review.\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"_Auto-trading resets tomorrow at 00:00 UTC._\n"
                    f"_Type `auto on` to override if desired._"
                )

        return True
    except Exception as e:
        log.error(f"MetaAPI close_trade error: {e}")
        return False

def move_sl_to_breakeven(trade_id):
    """Move SL to entry price (breakeven) via MetaAPI."""
    if trade_id not in auto_trades:
        return
    trade = auto_trades[trade_id]
    if trade.get("sl_moved"):
        return
    try:
        url = f"{META_API_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions/{trade_id}"
        payload = {"stopLoss": trade["entry"]}
        r = requests.put(url, headers=metaapi_headers(), json=payload, timeout=15)
        log.info(f"Move SL to BE for {trade_id}: {r.status_code}")
        trade["sl_moved"] = True
        trade["sl"] = trade["entry"]
    except Exception as e:
        log.error(f"MetaAPI move_sl error: {e}")

# ─── AUTO-TRADE MONITOR ───────────────────────────────────
def monitor_auto_trades(inst, current_price):
    """Check all open auto-trades for TP/SL/safety hits."""
    if not auto_trades:
        return
    dec = inst["decimals"]
    trades_to_close = []

    for trade_id, trade in list(auto_trades.items()):
        if trade["inst_id"] != inst["id"]:
            continue
        if trade["status"] != "OPEN":
            continue

        direction = trade["direction"]
        entry     = trade["entry"]
        sl        = trade["sl"]
        sl_dist   = trade["sl_dist"]
        tp1       = trade["tp1"]
        tp2       = trade["tp2"]
        tp3       = trade["tp3"]
        price     = current_price

        # ── Safety close: within 20% of SL ──
        if not trade["tp1_hit"]:
            dist_to_sl = abs(price - sl)
            if dist_to_sl <= sl_dist * SAFETY_CLOSE_PCT:
                send_telegram(
                    f"⚠️ *SAFETY CLOSE — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` is dangerously close to SL `{sl}`\n"
                    f"Auto-closing trade to protect capital.\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"_Trade closed before SL hit._"
                )
                trades_to_close.append((trade_id, "safety_close"))
                continue

        if direction == "BUY":
            # SL hit
            if price <= sl:
                send_telegram(
                    f"🔴 *AUTO-TRADE SL HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` hit SL `{sl}`\n"
                    f"Entry was `{entry}` | Lots: `{trade['lots']}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"_Loss taken. Stay disciplined._"
                )
                trades_to_close.append((trade_id, "sl_hit"))
            # TP1
            elif not trade["tp1_hit"] and price >= tp1:
                trade["tp1_hit"] = True
                move_sl_to_breakeven(trade_id)
                send_telegram(
                    f"✅ *AUTO-TRADE TP1 HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` reached TP1 `{tp1}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"🔒 SL moved to breakeven `{entry}`\n"
                    f"🎯 TP2 target: `{tp2}`\n"
                    f"🎯 TP3 target: `{tp3}`\n"
                    f"_Trade is now risk-free._"
                )
            # TP2
            elif trade["tp1_hit"] and not trade["tp2_hit"] and price >= tp2:
                trade["tp2_hit"] = True
                send_telegram(
                    f"✅ *AUTO-TRADE TP2 HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` reached TP2 `{tp2}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"🎯 TP3 still open at `{tp3}`\n"
                    f"_Consider closing partial or letting runner go._"
                )
            # TP3
            elif trade["tp2_hit"] and price >= tp3:
                send_telegram(
                    f"🏆 *AUTO-TRADE TP3 HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` reached TP3 `{tp3}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"_Full target reached! Outstanding trade._"
                )
                trades_to_close.append((trade_id, "tp3_hit"))

        elif direction == "SELL":
            # SL hit
            if price >= sl:
                send_telegram(
                    f"🔴 *AUTO-TRADE SL HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` hit SL `{sl}`\n"
                    f"Entry was `{entry}` | Lots: `{trade['lots']}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"_Loss taken. Stay disciplined._"
                )
                trades_to_close.append((trade_id, "sl_hit"))
            # TP1
            elif not trade["tp1_hit"] and price <= tp1:
                trade["tp1_hit"] = True
                move_sl_to_breakeven(trade_id)
                send_telegram(
                    f"✅ *AUTO-TRADE TP1 HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` reached TP1 `{tp1}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"🔒 SL moved to breakeven `{entry}`\n"
                    f"🎯 TP2 target: `{tp2}`\n"
                    f"🎯 TP3 target: `{tp3}`\n"
                    f"_Trade is now risk-free._"
                )
            # TP2
            elif trade["tp1_hit"] and not trade["tp2_hit"] and price <= tp2:
                trade["tp2_hit"] = True
                send_telegram(
                    f"✅ *AUTO-TRADE TP2 HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` reached TP2 `{tp2}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"🎯 TP3 still open at `{tp3}`\n"
                    f"_Consider closing partial or letting runner go._"
                )
            # TP3
            elif trade["tp2_hit"] and price <= tp3:
                send_telegram(
                    f"🏆 *AUTO-TRADE TP3 HIT — {trade['inst_id']}*\n"
                    f"Price `{round(price, dec)}` reached TP3 `{tp3}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"_Full target reached! Outstanding trade._"
                )
                trades_to_close.append((trade_id, "tp3_hit"))

    # Process closes
    for trade_id, reason in trades_to_close:
        close_auto_trade(trade_id, reason)

# ─── REST PRICE FALLBACK ──────────────────────────────────
def fetch_twelvedata(symbol):
    try:
        url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TWELVEDATA_API_KEY}"
        r = requests.get(url, timeout=10)
        d = r.json()
        if "price" in d:
            return float(d["price"])
    except Exception as e:
        log.error(f"TwelveData REST error {symbol}: {e}")
    return None

# ─── WEBSOCKET HANDLERS ───────────────────────────────────
def on_ws_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("event") == "price":
            symbol = data.get("symbol")
            price  = data.get("price")
            if symbol in SYMBOL_TO_ID and price:
                inst_id  = SYMBOL_TO_ID[symbol]
                inst_obj = next((i for i in INSTRUMENTS if i["id"] == inst_id), None)
                fp = float(price)
                if inst_obj:
                    vmin = inst_obj.get("valid_min", 0)
                    vmax = inst_obj.get("valid_max", 999999)
                    if vmin <= fp <= vmax:
                        latest_price[inst_id] = fp
                else:
                    latest_price[inst_id] = fp
        elif data.get("event") == "subscribe-status":
            log.info(f"WS subscribe status: {data.get('status')}")
    except Exception as e:
        log.error(f"WS message error: {e}")

def on_ws_error(ws, error):
    log.error(f"WS error: {error}")
    stats["ws_connected"] = False

def on_ws_close(ws, code, msg):
    log.warning(f"WS closed: {code} {msg}")
    stats["ws_connected"] = False

def on_ws_open(ws):
    log.info("WebSocket connected")
    stats["ws_connected"] = True
    symbols = ",".join(i["symbol"] for i in INSTRUMENTS if i.get("ws"))
    sub = {"action": "subscribe", "params": {"symbols": symbols}}
    ws.send(json.dumps(sub))
    log.info(f"WS Subscribed to: {symbols}")

def run_websocket():
    if websocket is None:
        log.error("websocket-client not installed")
        return
    url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVEDATA_API_KEY}"
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_ws_open,
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close,
            )
            ws.run_forever(ping_interval=10, ping_timeout=5)
        except Exception as e:
            log.error(f"WS run error: {e}")
        stats["ws_connected"] = False
        log.info("Reconnecting WebSocket in 5s...")
        time.sleep(5)

def price_is_valid(inst, price):
    if price is None:
        return False
    vmin = inst.get("valid_min", 0)
    vmax = inst.get("valid_max", 999999)
    if price < vmin or price > vmax:
        log.warning(f"{inst['id']} price {price} outside range [{vmin}-{vmax}]")
        return False
    return True

def rest_poll_loop():
    while True:
        for inst in INSTRUMENTS:
            if inst.get("ws"):
                continue
            if inst["id"] in paused_markets:
                continue
            price = fetch_twelvedata(inst["symbol"])
            if price is not None and inst.get("multiplier"):
                price = price * inst["multiplier"]
            if price_is_valid(inst, price):
                latest_price[inst["id"]] = price
            elif price is not None:
                if not feed_alerted.get(inst["id"]):
                    feed_alerted[inst["id"]] = True
                    send_telegram(
                        f"⚠️ *{inst['id']} Data Warning*\n"
                        f"Received price `{price}` outside expected range.\n"
                        f"Signals for {inst['id']} paused until valid data returns."
                    )
        time.sleep(8)

# ─── CHART PATTERN DETECTION ──────────────────────────────
def detect_pattern(prices):
    if len(prices) < 20:
        return None, None
    p = prices[-20:]
    high, low = max(p), min(p)
    mid = (high + low) / 2
    last = p[-1]
    peaks   = [i for i in range(1, len(p)-1) if p[i] > p[i-1] and p[i] > p[i+1]]
    troughs = [i for i in range(1, len(p)-1) if p[i] < p[i-1] and p[i] < p[i+1]]

    if len(peaks) >= 2 and len(troughs) >= 1:
        p1, p2 = p[peaks[-2]], p[peaks[-1]]
        t1 = p[troughs[-1]]
        if abs(p1 - p2) / p1 < 0.003 and last < t1:
            return "Double Top", "SELL"
        if len(peaks) >= 3:
            left, head, right = p[peaks[-3]], p[peaks[-2]], p[peaks[-1]]
            if head > left and head > right and abs(left - right)/left < 0.005 and last < t1:
                return "Head & Shoulders", "SELL"

    if len(troughs) >= 2 and len(peaks) >= 1:
        t1_v, t2_v = p[troughs[-2]], p[troughs[-1]]
        peak_v = p[peaks[-1]]
        if abs(t1_v - t2_v)/t1_v < 0.003 and last > peak_v:
            return "Double Bottom", "BUY"
        if len(troughs) >= 3:
            left, head, right = p[troughs[-3]], p[troughs[-2]], p[troughs[-1]]
            if head < left and head < right and abs(left - right)/left < 0.005 and last > peak_v:
                return "Inverted H&S", "BUY"

    recent_5 = p[-5:]
    prev_5   = p[-10:-5]
    if max(prev_5) > min(prev_5) * 1.003:
        if max(recent_5) < max(prev_5) and min(recent_5) > min(prev_5) * 0.999:
            return "Bull Flag", "BUY"
    if min(prev_5) < max(prev_5) * 0.997:
        if min(recent_5) > min(prev_5) and max(recent_5) < max(prev_5) * 1.001:
            return "Bear Flag", "SELL"
    if last > mid and (high - low)/last < 0.005:
        return "Bullish Wedge", "BUY"
    if last < mid and (high - low)/last < 0.005:
        return "Bearish Wedge", "SELL"
    return None, None

def detect_liquidity_sweep(prices):
    if len(prices) < 15:
        return None
    recent   = prices[-5:]
    lookback = prices[-15:-5]
    prev_high, prev_low = max(lookback), min(lookback)
    last, prev = prices[-1], prices[-2]
    if max(recent[:-1]) > prev_high and last < prev_high and last < prev:
        return "SELL"
    if min(recent[:-1]) < prev_low and last > prev_low and last > prev:
        return "BUY"
    return None

def get_structure(prices):
    if len(prices) < 10:
        return "UNKNOWN"
    recent = prices[-10:]
    highs = [recent[i] for i in range(1, len(recent)-1) if recent[i] > recent[i-1] and recent[i] > recent[i+1]]
    lows  = [recent[i] for i in range(1, len(recent)-1) if recent[i] < recent[i-1] and recent[i] < recent[i+1]]
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
            return "BULLISH"
        if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
            return "BEARISH"
    return "RANGING"

def rate_signal(factors):
    score = sum(factors.values())
    total = len(factors)
    if score >= 4 and (score / total) >= 0.7:
        return "STRONG 🔥"
    elif score >= 3:
        return "MID ⚡"
    return None

def analyze(prices, inst):
    if len(prices) < 12:
        return {"signal": "WAIT", "reason": "Building history..."}
    last, prev = prices[-1], prices[-2]
    momentum = last - prev
    recent = prices[-12:]
    high, low = max(recent), min(recent)
    rng_pct = ((high - low) / last) * 100
    dec, sl = inst["decimals"], inst["sl_dist"]

    min_range = 0.04 if inst["id"] == "EURUSD" else 0.14
    if rng_pct < min_range:
        return {"signal": "WAIT", "reason": "Consolidating — no volatility"}
    if abs(momentum) > inst["sl_dist"] * 1.8:
        return {"signal": "WAIT", "reason": "Post-spike — wait for structure"}

    structure = get_structure(prices)
    liq_sweep = detect_liquidity_sweep(prices)
    pattern, pattern_dir = detect_pattern(prices)

    mid = (high + low) / 2
    h_recent, h_old = recent[-4:], recent[:4]
    bull_struct = h_recent[-1] > h_old[-1] and last > mid
    bear_struct = h_recent[-1] < h_old[-1] and last < mid
    bull_mom = momentum > 0 and (last - prices[-6]) > 0
    bear_mom = momentum < 0 and (last - prices[-6]) < 0
    prev_high = max(prices[-15:-3]) if len(prices) >= 15 else high
    prev_low  = min(prices[-15:-3]) if len(prices) >= 15 else low
    demand_retest = last <= prev_low * 1.0008 and bull_mom
    supply_retest = last >= prev_high * 0.9992 and bear_mom

    buy_signal = sell_signal = False
    reasons = []
    if liq_sweep == "BUY":    buy_signal  = True; reasons.append("Liquidity sweep reversal")
    elif liq_sweep == "SELL": sell_signal = True; reasons.append("Liquidity sweep reversal")
    if pattern_dir == "BUY":    buy_signal  = True; reasons.append(f"{pattern} pattern")
    elif pattern_dir == "SELL": sell_signal = True; reasons.append(f"{pattern} pattern")
    if bull_struct and bull_mom and demand_retest:
        buy_signal  = True; reasons.append("HH/HL + demand retest")
    if bear_struct and bear_mom and supply_retest:
        sell_signal = True; reasons.append("LH/LL + supply retest")

    if not buy_signal and not sell_signal:
        return {"signal": "WAIT", "reason": "No confluence setup"}
    if buy_signal and sell_signal:
        return {"signal": "WAIT", "reason": "Conflicting signals"}

    direction = "BUY" if buy_signal else "SELL"
    struct_aligned = (
        (direction == "BUY"  and structure in ("BULLISH", "UNKNOWN")) or
        (direction == "SELL" and structure in ("BEARISH", "UNKNOWN"))
    )
    factors = {
        "liquidity_sweep": liq_sweep is not None,
        "pattern":         pattern is not None,
        "structure":       (bull_struct if direction == "BUY" else bear_struct),
        "momentum":        (bull_mom if direction == "BUY" else bear_mom),
        "retest":          (demand_retest if direction == "BUY" else supply_retest),
        "struct_aligned":  struct_aligned,
    }
    confirm_candle = (
        (direction == "BUY"  and prices[-1] > prices[-2]) or
        (direction == "SELL" and prices[-1] < prices[-2])
    )
    factors["confirm_candle"] = confirm_candle

    rating = rate_signal(factors)
    if not rating:
        return {"signal": "WAIT", "reason": "Setup too weak — skipping"}
    if not confirm_candle:
        return {"signal": "WAIT", "reason": "Awaiting reaction candle confirmation"}

    tp1_mult, tp2_mult = 1.0, 1.8
    if "STRONG" in rating:
        tp2_mult = 2.2

    if direction == "BUY":
        return {"signal": "BUY", "rating": rating, "entry": round(last, dec),
                "sl": round(last - sl, dec), "tp1": round(last + sl*tp1_mult, dec),
                "tp2": round(last + sl*tp2_mult, dec), "rr": f"1:{tp1_mult}",
                "reason": " + ".join(reasons), "structure": structure}
    else:
        return {"signal": "SELL", "rating": rating, "entry": round(last, dec),
                "sl": round(last + sl, dec), "tp1": round(last - sl*tp1_mult, dec),
                "tp2": round(last - sl*tp2_mult, dec), "rr": f"1:{tp1_mult}",
                "reason": " + ".join(reasons), "structure": structure}

# ─── LOT SIZE + PROFIT ────────────────────────────────────
def get_lot_recommendation(rating, inst):
    if "STRONG" in rating: return inst.get("lot_strong", 0.03)
    elif "MID" in rating:  return inst.get("lot_mid", 0.02)
    return 0.01

def estimate_profit(inst_id, entry, tp1, tp2, sl, lots):
    try:
        d_tp1 = abs(float(tp1) - float(entry))
        d_tp2 = abs(float(tp2) - float(entry))
        d_sl  = abs(float(sl)  - float(entry))
        if inst_id == "XAUUSD":
            pv = 100 * lots
        elif inst_id == "NAS100":
            pv = 1 * lots
        elif inst_id == "USOUSD":
            pv = 1000 * lots
        elif inst_id == "EURUSD":
            return (round(100000*lots*d_tp1,2), round(100000*lots*d_tp2,2), round(100000*lots*d_sl,2))
        else:
            return 0.0, 0.0, 0.0
        return round(d_tp1*pv,2), round(d_tp2*pv,2), round(d_sl*pv,2)
    except:
        return 0.0, 0.0, 0.0

def format_signal(inst, a):
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    emoji  = "🟢" if a["signal"] == "BUY" else "🔴"
    rating = a.get("rating", "MID ⚡")
    lots   = get_lot_recommendation(rating, inst)
    p1, p2, ls = estimate_profit(inst["id"], a["entry"], a["tp1"], a["tp2"], a["sl"], lots)
    return (
        f"{emoji} *{a['signal']} — {inst['id']}* ({inst['label']})\n"
        f"⭐ Rating: *{rating}*\n"
        f"⏰ `{now}`\n"
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
        f"💡 _Tip: Move SL to breakeven after TP1 hits._\n"
        f"⚡ _XAUUSD can spike during session opens & news — confirm momentum before entry._\n"
        f"⚠️ _Confirm on chart before trading_"
    )

# ─── AUTO-TRADE STATUS FORMATTERS ─────────────────────────
def format_auto_positions():
    if not auto_trades:
        return "📭 *No open auto-trades right now.*"
    lines = ["🤖 *Open Auto-Trade Positions*\n"]
    for tid, t in auto_trades.items():
        price = latest_price.get(t["inst_id"])
        price_str = str(round(price, 2)) if price else "N/A"
        tp1_status = "✅" if t["tp1_hit"] else "⏳"
        tp2_status = "✅" if t["tp2_hit"] else "⏳"
        sl_status  = "🔒 BE" if t["sl_moved"] else f"`{t['sl']}`"
        emoji = "🟢" if t["direction"] == "BUY" else "🔴"
        lines.append(
            f"{emoji} *{t['inst_id']}* {t['direction']}\n"
            f"📍 Entry: `{t['entry']}` | Now: `{price_str}`\n"
            f"🛑 SL: {sl_status}\n"
            f"🎯 TP1: `{t['tp1']}` {tp1_status}\n"
            f"🎯 TP2: `{t['tp2']}` {tp2_status}\n"
            f"🎯 TP3: `{t['tp3']}` ⏳\n"
            f"💼 Lots: `{t['lots']}` | Opened: `{t['opened_at']}`\n"
            f"━━━━━━━━━━━━━━"
        )
    return "\n".join(lines)

def format_auto_history():
    if not auto_trade_history:
        return "📭 *No closed auto-trades yet.*"
    lines = ["📜 *Last Closed Auto-Trades*\n"]
    for t in reversed(auto_trade_history[-5:]):
        emoji = "🟢" if t["direction"] == "BUY" else "🔴"
        reason_map = {
            "tp3_hit":     "🏆 TP3 Hit",
            "tp2_hit":     "✅ TP2 Hit",
            "tp1_hit":     "✅ TP1 Hit",
            "sl_hit":      "🔴 SL Hit",
            "safety_close":"⚠️ Safety Close",
            "manual":      "🖐 Manual Close",
        }
        outcome = reason_map.get(t.get("close_reason", ""), t.get("close_reason", "Closed"))
        lines.append(
            f"{emoji} *{t['inst_id']}* {t['direction']} — {outcome}\n"
            f"Entry `{t['entry']}` | Opened `{t['opened_at']}` | Closed `{t.get('closed_at','N/A')}`\n"
            f"━━━━━━━━━━━━━━"
        )
    return "\n".join(lines)

# ─── COMMANDS ─────────────────────────────────────────────
def check_telegram_commands():
    global last_update_id, AUTO_TRADE_ENABLED
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?timeout=1&offset={last_update_id+1}&limit=10"
        r = requests.get(url, timeout=6)
        for u in r.json().get("result", []):
            last_update_id = u.get("update_id", last_update_id)
            msg = u.get("message", {}).get("text", "").strip().lower()

            # ── Auto-trade commands ──
            if msg in ("auto positions", "auto position"):
                send_telegram(format_auto_positions())

            elif msg in ("auto history",):
                send_telegram(format_auto_history())

            elif msg == "auto off":
                AUTO_TRADE_ENABLED = False
                send_telegram("⏸ *Auto-trading DISABLED.*\nSignals will still fire — trades won't be placed automatically.")

            elif msg == "auto on":
                AUTO_TRADE_ENABLED = True
                send_telegram("▶️ *Auto-trading ENABLED.*\nSTRONG signals during London/NY will be placed automatically.")

            elif msg == "auto status":
                session_active = is_london_ny_session()
                status_str     = "✅ ENABLED" if AUTO_TRADE_ENABLED else "⏸ DISABLED"
                session_str    = "✅ London/NY Active" if session_active else "⏳ Outside auto-trade hours"
                kill_str       = "🚨 ACTIVE — losses limit hit" if stats["kill_switch_active"] else "✅ Clear"
                balance_str    = f"${stats['account_balance']:.2f}" if stats["account_balance"] else "Fetching..."
                open_count     = len(auto_trades)
                lot            = calculate_lot_size(3.5, "XAUUSD")
                send_telegram(
                    f"🤖 *Auto-Trade Status*\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"Switch: {status_str}\n"
                    f"Session: {session_str}\n"
                    f"Kill-switch: {kill_str}\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"💰 Account balance: `{balance_str}`\n"
                    f"📊 Risk per trade: `{int(RISK_PCT*100)}% of balance`\n"
                    f"💼 Current lot size: `{lot}` _(dynamic)_\n"
                    f"🔒 Max positions: `{MAX_OPEN_POSITIONS}`\n"
                    f"🛑 Max daily losses: `{MAX_DAILY_LOSSES}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"📈 Open trades: `{open_count}`\n"
                    f"📨 Trades today: `{stats['auto_trades_today']}`\n"
                    f"❌ Losses today: `{stats['daily_losses']}/{MAX_DAILY_LOSSES}`\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"_STRONG XAUUSD signals only, London + NY sessions_"
                )

            # ── Standard commands ──
            elif msg in ("/status", "status"):
                send_status()

            elif msg in ("/help", "help", "prompts"):
                send_telegram(
                    "📋 *SMC Bot Commands*\n\n"
                    "*Signals & Markets*\n"
                    "`status` — Live bot status\n"
                    "`markets` — Active markets with prices\n"
                    "`pause XAUUSD` — Pause Gold signals\n"
                    "`pause all` — Pause all markets\n"
                    "`resume XAUUSD` — Resume Gold signals\n"
                    "`resume all` — Resume all markets\n"
                    "`xauusd` — Live Gold price\n"
                    "`nas100` — Live Nasdaq price\n"
                    "`eurusd` — Live EUR/USD price\n"
                    "`usousd` — Live Oil price\n\n"
                    "*Auto-Trading*\n"
                    "`auto positions` — Open auto-trades\n"
                    "`auto history` — Last 5 closed trades\n"
                    "`auto status` — Auto-trade on/off + session\n"
                    "`auto on` — Enable auto-trading\n"
                    "`auto off` — Disable auto-trading\n\n"
                    "`help` — Show this menu"
                )

            elif msg.startswith("pause"):
                parts = msg.split()
                t = parts[1].upper() if len(parts) > 1 else "ALL"
                if t == "ALL":
                    for i in INSTRUMENTS: paused_markets.add(i["id"])
                    send_telegram("⏸ *All markets paused.*")
                elif t in [i["id"] for i in INSTRUMENTS]:
                    paused_markets.add(t)
                    send_telegram(f"⏸ *{t}* paused.")
                else:
                    send_telegram(f"❓ Unknown: `{t}`")

            elif msg.startswith("resume"):
                parts = msg.split()
                t = parts[1].upper() if len(parts) > 1 else "ALL"
                if t == "ALL":
                    paused_markets.clear()
                    send_telegram("▶️ *All markets resumed.*")
                else:
                    paused_markets.discard(t)
                    send_telegram(f"▶️ *{t}* resumed.")

            elif msg in ("/markets", "markets"):
                lines = []
                for i in INSTRUMENTS:
                    st = "⏸ PAUSED" if i["id"] in paused_markets else "✅ Active"
                    pr = latest_price[i["id"]]
                    pr = round(pr, i["decimals"]) if pr else "N/A"
                    lines.append(f"{st} — `{i['id']}` @ `{pr}`")
                send_telegram("📊 *Market Status*\n\n" + "\n".join(lines))

            else:
                inst_lookup = {i["id"].lower(): i for i in INSTRUMENTS}
                if msg in inst_lookup:
                    i = inst_lookup[msg]
                    price = latest_price[i["id"]]
                    if price:
                        dec    = i["decimals"]
                        spread = i["sl_dist"] * 0.1
                        buy_p  = round(price + spread, dec)
                        sell_p = round(price - spread, dec)
                        send_telegram(
                            f"*{i['id']}* ({i['label']})\n"
                            f"━━━━━━━━━━━━━━\n"
                            f"BUY:  `{buy_p}`\n"
                            f"SELL: `{sell_p}`\n"
                            f"_Live price — approximate spread_"
                        )
                    else:
                        send_telegram(f"No price data yet for `{i['id']}` — try again shortly.")
    except Exception as e:
        log.warning(f"Command error: {e}")

# ─── SESSION HELPERS ──────────────────────────────────────
def get_active_session(now):
    h = now.hour
    s = []
    if 7  <= h < 16: s.append("London 🇬🇧")
    if 12 <= h < 21: s.append("New York 🇺🇸")
    if 0  <= h < 9:  s.append("Tokyo 🇯🇵")
    if h >= 22 or h < 7: s.append("Sydney 🇦🇺")
    return " + ".join(s) if s else "Off-hours"

def send_status():
    now = datetime.now(timezone.utc)
    up  = now - stats["start_time"]
    h, rem = divmod(int(up.total_seconds()), 3600)
    m   = rem // 60
    ls  = stats["last_scan"].strftime("%H:%M:%S UTC") if stats["last_scan"] else "N/A"
    lsig = stats["last_signal_sent"].strftime("%H:%M UTC") if stats["last_signal_sent"] else "None today"
    feed = "🟢 WebSocket Live" if stats["ws_connected"] else "🟡 REST Fallback"
    auto_str = "✅ ON" if AUTO_TRADE_ENABLED else "⏸ OFF"
    send_telegram(
        f"✅ *Bot Online*\n"
        f"🕐 Time: `{now.strftime('%H:%M:%S UTC')}`\n"
        f"⏱ Uptime: `{h}h {m}m`\n"
        f"📡 Feed: {feed}\n"
        f"🤖 Auto-Trade: {auto_str}\n"
        f"━━━━━━━━━━━━━━\n"
        f"📊 Markets: `XAUUSD | NAS100 | EURUSD | USOUSD`\n"
        f"🕵️ Last analysis: `{ls}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"🌍 Session: `{get_active_session(now)}`\n"
        f"📨 Signals today: `{stats['signals_today']}`\n"
        f"🤖 Auto-trades today: `{stats['auto_trades_today']}`\n"
        f"⏰ Last signal: `{lsig}`"
    )

SESSION_MSGS = {
    "London_open":   ("🇬🇧 *London Session Open*\n`07:00 UTC` — Prime window. Watch for setups.\n🤖 Auto-trading ACTIVE for STRONG Gold signals.", 7),
    "NewYork_open":  ("🇺🇸 *New York Session Open*\n`12:00 UTC` — High volatility. Best overlap.\n🤖 Auto-trading ACTIVE for STRONG Gold signals.", 12),
    "London_close":  ("🇬🇧 *London Closing*\n`16:00 UTC` — Liquidity dropping.", 16),
    "NewYork_close": ("🇺🇸 *New York Closing*\n`21:00 UTC` — Markets winding down.\n🤖 Auto-trading PAUSED until next session.", 21),
    "Asian_open":    ("🌏 *Asian Session*\n`00:00 UTC` — Lower liquidity. Manual trades only.", 0),
}

def check_session_announcements():
    now = datetime.now(timezone.utc)
    if now.minute > 5:
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
    feed = "🟢 WebSocket" if stats["ws_connected"] else "🟡 REST"
    ps = ""
    for i in INSTRUMENTS:
        if latest_price[i["id"]]:
            ps += f"`{i['id']}: {round(latest_price[i['id']], i['decimals'])}` "
    send_telegram(
        f"💓 *Heartbeat* `{now.strftime('%H:%M UTC')}`\n"
        f"No signals in 20 mins. Feed: {feed}\n"
        f"Session: `{get_active_session(now)}`\n"
        f"{ps}"
    )
    stats["last_heartbeat"] = now

def reset_daily_stats():
    global last_reset_day
    today = datetime.now(timezone.utc).date()
    if last_reset_day != today:
        stats["signals_today"]      = 0
        stats["auto_trades_today"]  = 0
        stats["daily_losses"]       = 0       # Reset loss counter
        stats["kill_switch_active"] = False   # Reset kill-switch
        last_reset_day = today
        announced_sessions.clear()
        log.info("Daily stats reset — kill-switch cleared")

# ─── MANUAL TP/SL MONITOR ─────────────────────────────────
def check_tp_sl(inst, current_price):
    inst_id = inst["id"]
    if inst_id not in open_signals:
        return
    sig = open_signals[inst_id]
    direction = sig["direction"]
    dec   = inst["decimals"]
    price = current_price

    if direction == "BUY":
        if not sig["tp1_hit"] and price >= sig["tp1"]:
            sig["tp1_hit"] = True
            send_telegram(
                f"*TP1 HIT — {inst_id}* ({inst['label']})\n"
                f"Price reached `{round(price, dec)}`\n"
                f"TP1 was `{sig['tp1']}`\n"
                f"━━━━━━━━━━━━━━\n"
                f"_Move SL to breakeven now._\n"
                f"TP2 still open at `{sig['tp2']}`"
            )
        elif sig["tp1_hit"] and price >= sig["tp2"]:
            send_telegram(
                f"*TP2 HIT — {inst_id}* ({inst['label']})\n"
                f"Price reached `{round(price, dec)}`\n"
                f"Full target reached — close trade!"
            )
            del open_signals[inst_id]
        elif price <= sig["sl"]:
            send_telegram(
                f"*SL HIT — {inst_id}* ({inst['label']})\n"
                f"Price dropped to `{round(price, dec)}`\n"
                f"SL was `{sig['sl']}`\n"
                f"_Small loss — stay disciplined._"
            )
            del open_signals[inst_id]

    elif direction == "SELL":
        if not sig["tp1_hit"] and price <= sig["tp1"]:
            sig["tp1_hit"] = True
            send_telegram(
                f"*TP1 HIT — {inst_id}* ({inst['label']})\n"
                f"Price reached `{round(price, dec)}`\n"
                f"TP1 was `{sig['tp1']}`\n"
                f"━━━━━━━━━━━━━━\n"
                f"_Move SL to breakeven now._\n"
                f"TP2 still open at `{sig['tp2']}`"
            )
        elif sig["tp1_hit"] and price <= sig["tp2"]:
            send_telegram(
                f"*TP2 HIT — {inst_id}* ({inst['label']})\n"
                f"Price reached `{round(price, dec)}`\n"
                f"Full target reached — close trade!"
            )
            del open_signals[inst_id]
        elif price >= sig["sl"]:
            send_telegram(
                f"*SL HIT — {inst_id}* ({inst['label']})\n"
                f"Price rose to `{round(price, dec)}`\n"
                f"SL was `{sig['sl']}`\n"
                f"_Small loss — stay disciplined._"
            )
            del open_signals[inst_id]

# ─── ANALYSIS LOOP ────────────────────────────────────────
def analysis_loop():
    while True:
        try:
            stats["last_scan"] = datetime.now(timezone.utc)
            for inst in INSTRUMENTS:
                if inst["id"] in paused_markets:
                    continue
                price = latest_price[inst["id"]]
                if price and price > 0:
                    hist = price_history[inst["id"]]
                    if not hist or hist[-1] != price:
                        hist.append(price)
                        if len(hist) > 150:
                            hist.pop(0)
                    # Monitor manual TP/SL
                    check_tp_sl(inst, price)
                    # Monitor auto-trades
                    monitor_auto_trades(inst, price)

                analysis = analyze(price_history[inst["id"]], inst)
                sig      = analysis["signal"]
                prev_sig = last_signals[inst["id"]]
                log.info(f"{inst['id']} | {price} | {sig} | {analysis.get('reason','')}")

                if sig != "WAIT" and sig != prev_sig:
                    now_t  = datetime.now(timezone.utc)
                    last_t = last_signal_time[inst["id"]]
                    cd     = COOLDOWNS.get(inst["id"], 600)
                    in_cooldown = last_t and (now_t - last_t).total_seconds() < cd

                    if in_cooldown:
                        remaining = int((cd - (now_t - last_t).total_seconds()) / 60)
                        log.info(f"{inst['id']} | {sig} suppressed — cooldown {remaining}m left")
                        last_signals[inst["id"]] = sig
                    else:
                        last_signals[inst["id"]] = sig
                        last_signal_time[inst["id"]] = now_t
                        rating = analysis.get("rating", "")

                        # ── AUTO-TRADE: STRONG + XAUUSD + London/NY only ──
                        auto_placed = False
                        if (
                            AUTO_TRADE_ENABLED
                            and inst["id"] == AUTO_TRADE_SYMBOL
                            and "STRONG" in rating
                            and is_london_ny_session()
                            and METAAPI_TOKEN
                            and METAAPI_ACCOUNT_ID
                        ):
                            trade_id = place_auto_trade(inst, analysis)
                            if trade_id:
                                auto_placed = True
                                stats["auto_trades_today"] += 1
                                sl_dist  = inst["sl_dist"]
                                dec      = inst["decimals"]
                                entry    = analysis["entry"]
                                direction = sig

                                if direction == "BUY":
                                    tp1 = round(entry + sl_dist * AUTO_TP1_MULT, dec)
                                    tp2 = round(entry + sl_dist * AUTO_TP2_MULT, dec)
                                    tp3 = round(entry + sl_dist * AUTO_TP3_MULT, dec)
                                else:
                                    tp1 = round(entry - sl_dist * AUTO_TP1_MULT, dec)
                                    tp2 = round(entry - sl_dist * AUTO_TP2_MULT, dec)
                                    tp3 = round(entry - sl_dist * AUTO_TP3_MULT, dec)

                                emoji = "🟢" if direction == "BUY" else "🔴"
                                actual_lot = auto_trades.get(trade_id, {}).get("lots", FALLBACK_LOT)
                                send_telegram(
                                    f"🤖 *AUTO-TRADE PLACED — {inst['id']}*\n"
                                    f"{emoji} *{direction}* | ⭐ STRONG 🔥\n"
                                    f"━━━━━━━━━━━━━━\n"
                                    f"📍 Entry:  `{entry}`\n"
                                    f"🛑 SL:     `{analysis['sl']}`\n"
                                    f"🎯 TP1:    `{tp1}` _(easy target)_\n"
                                    f"🎯 TP2:    `{tp2}`\n"
                                    f"🎯 TP3:    `{tp3}` _(runner)_\n"
                                    f"💼 Lots:   `{actual_lot}` _(1% risk)_\n"
                                    f"━━━━━━━━━━━━━━\n"
                                    f"🔒 SL moves to breakeven after TP1\n"
                                    f"⚠️ Safety close if price nears SL\n"
                                    f"_Type `auto positions` to monitor_"
                                )
                            else:
                                send_telegram(
                                    f"⚠️ *AUTO-TRADE FAILED — {inst['id']}*\n"
                                    f"Could not place trade via MetaAPI.\n"
                                    f"Signal still valid — place manually if confirmed.\n"
                                    f"Entry: `{analysis['entry']}` | SL: `{analysis['sl']}`"
                                )

                        # Always send the standard signal message
                        send_telegram(format_signal(inst, analysis))
                        stats["signals_today"]   += 1
                        stats["last_signal_sent"] = now_t
                        stats["last_heartbeat"]   = now_t

                        # Store for manual TP/SL monitoring
                        open_signals[inst["id"]] = {
                            "direction": sig,
                            "entry":     analysis["entry"],
                            "sl":        analysis["sl"],
                            "tp1":       analysis["tp1"],
                            "tp2":       analysis["tp2"],
                            "tp1_hit":   False,
                        }

                elif sig == "WAIT" and prev_sig in ("BUY", "SELL"):
                    last_signals[inst["id"]] = "WAIT"
                    send_telegram(
                        f"⏸ *{inst['id']}* signal cleared — now `WAIT`\n"
                        f"_Previous signal no longer valid._"
                    )

            reset_daily_stats()
            check_session_announcements()
            check_heartbeat()
            check_telegram_commands()
        except Exception as e:
            log.error(f"Analysis loop error: {e}")
        time.sleep(ANALYZE_INTERVAL)

# ─── MAIN ─────────────────────────────────────────────────
def main():
    log.info("SMC Signal Bot v4 (Auto-Trade + Safety) — Starting")

    # Fetch account balance on startup for dynamic lot sizing
    if METAAPI_TOKEN and METAAPI_ACCOUNT_ID:
        balance = fetch_account_balance()
        if balance:
            lot = calculate_lot_size(3.5, "XAUUSD")
            log.info(f"Startup: balance=${balance:.2f}, lot={lot}")
        else:
            log.warning("Could not fetch balance — using fallback lot size")

    auto_status = "✅ ENABLED" if (AUTO_TRADE_ENABLED and METAAPI_TOKEN) else "⚠️ DISABLED (no credentials)"
    balance_str = f"${stats['account_balance']:.2f}" if stats["account_balance"] else "Unknown"
    lot_str     = str(calculate_lot_size(3.5, "XAUUSD")) if stats["account_balance"] else str(FALLBACK_LOT)

    send_telegram(
        f"✅ *SMC Signal Bot v4 Online*\n"
        f"📊 Monitoring: `XAUUSD | NAS100 | EURUSD | USOUSD`\n"
        f"📡 XAUUSD + EURUSD: `WebSocket (real-time)`\n"
        f"📡 NAS100 + USOUSD: `REST (8s)`\n"
        f"⚡ Analysis every: `{ANALYZE_INTERVAL}s`\n"
        f"🤖 Auto-Trade: {auto_status}\n"
        f"━━━━━━━━━━━━━━\n"
        f"🛡️ *Safety Features Active*\n"
        f"💰 Balance: `{balance_str}` | Lot: `{lot_str}` _(1% risk)_\n"
        f"🔒 Max positions: `{MAX_OPEN_POSITIONS}`\n"
        f"🛑 Kill-switch: `{MAX_DAILY_LOSSES} losses = auto-stop`\n"
        f"━━━━━━━━━━━━━━\n"
        f"_STRONG XAUUSD signals → auto-placed during London/NY_\n"
        f"_MID signals + Asian session → manual only_\n"
        f"Type `help` for all commands."
    )

    if USE_WEBSOCKET and websocket:
        t_ws = threading.Thread(target=run_websocket, daemon=True)
        t_ws.start()
        log.info("WebSocket thread started")

    t_rest = threading.Thread(target=rest_poll_loop, daemon=True)
    t_rest.start()
    log.info("REST polling thread started")

    time.sleep(3)
    analysis_loop()

if __name__ == "__main__":
    main()
