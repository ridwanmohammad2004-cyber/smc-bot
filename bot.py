import os
import time
import requests
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
SCAN_INTERVAL      = int(os.environ.get("SCAN_INTERVAL", "30"))

# ─── INSTRUMENTS (PU Prime Islamic Standard) ──────────────
INSTRUMENTS = [
    {"id": "XAUUSD", "label": "Gold",    "symbol": "XAU/USD",  "decimals": 2,  "sl_dist": 3.5,   "td": True, "priority": 1},
    {"id": "NAS100", "label": "Nasdaq",  "symbol": "NDX",      "decimals": 1,  "sl_dist": 20.0,  "td": True, "priority": 2},
    {"id": "EURUSD", "label": "EUR/USD", "symbol": "EUR/USD",  "decimals": 5,  "sl_dist": 0.0015,"td": True, "priority": 3},
    {"id": "USOUSD", "label": "WTI Oil", "symbol": "USO",      "decimals": 2,  "sl_dist": 0.8,   "td": True, "priority": 4},
]

# ─── STATE ────────────────────────────────────────────────
price_history   = {i["id"]: [] for i in INSTRUMENTS}
last_signals    = {i["id"]: None for i in INSTRUMENTS}
last_signal_time= {i["id"]: None for i in INSTRUMENTS}
stats = {
    "signals_today": 0,
    "last_scan": None,
    "start_time": datetime.now(timezone.utc),
    "last_signal_sent": None,
    "last_heartbeat": None,
}
announced_sessions = set()
announced_market   = set()

# ─── SESSIONS ─────────────────────────────────────────────
SESSIONS = [
    {"name": "Sydney",  "open": 22, "close": 7,  "emoji": "🇦🇺"},
    {"name": "Tokyo",   "open": 0,  "close": 9,  "emoji": "🇯🇵"},
    {"name": "London",  "open": 7,  "close": 16, "emoji": "🇬🇧"},
    {"name": "New York","open": 12, "close": 21, "emoji": "🇺🇸"},
]

# ─── PRICE FETCHING ───────────────────────────────────────
def fetch_twelvedata(symbol):
    try:
        url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TWELVEDATA_API_KEY}"
        r = requests.get(url, timeout=10)
        d = r.json()
        if "price" in d:
            return float(d["price"])
    except Exception as e:
        log.error(f"TwelveData error {symbol}: {e}")
    return None

def fetch_yahoo(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=5m"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        return float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except:
        return None

def fetch_frankfurter():
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=EUR&to=USD", timeout=10)
        return float(r.json()["rates"]["USD"])
    except:
        return None

# Track data feed failures
feed_failures = {i["id"]: 0 for i in INSTRUMENTS}
feed_alerted  = {i["id"]: False for i in INSTRUMENTS}

def get_price(inst):
    """Fetch price from TwelveData only — no stale fallbacks."""
    if not TWELVEDATA_API_KEY:
        log.warning("No TwelveData API key configured")
        return None
    price = fetch_twelvedata(inst["symbol"])
    if price:
        feed_failures[inst["id"]] = 0
        if feed_alerted[inst["id"]]:
            feed_alerted[inst["id"]] = False
            send_telegram(f"✅ *{inst['id']}* data feed restored.")
        return price
    else:
        feed_failures[inst["id"]] += 1
        if feed_failures[inst["id"]] >= 3 and not feed_alerted[inst["id"]]:
            feed_alerted[inst["id"]] = True
            send_telegram(
                f"⚠️ *Data Feed Issue — {inst['id']}*\n"
                f"TwelveData not returning prices.\n"
                f"Check your API key or plan limits."
            )
        return None

# ─── CHART PATTERN DETECTION ──────────────────────────────
def detect_pattern(prices):
    """Detect basic chart patterns from recent price history."""
    if len(prices) < 20:
        return None, None

    p = prices[-20:]
    high = max(p)
    low  = min(p)
    mid  = (high + low) / 2
    last = p[-1]

    # Find local peaks and troughs
    peaks  = [i for i in range(1, len(p)-1) if p[i] > p[i-1] and p[i] > p[i+1]]
    troughs= [i for i in range(1, len(p)-1) if p[i] < p[i-1] and p[i] < p[i+1]]

    if len(peaks) >= 2 and len(troughs) >= 1:
        p1, p2 = p[peaks[-2]], p[peaks[-1]]
        t1 = p[troughs[-1]]

        # Double Top
        if abs(p1 - p2) / p1 < 0.003 and last < t1:
            return "Double Top", "SELL"

        # Head & Shoulders
        if len(peaks) >= 3:
            left, head, right = p[peaks[-3]], p[peaks[-2]], p[peaks[-1]]
            if head > left and head > right and abs(left - right) / left < 0.005:
                if last < t1:
                    return "Head & Shoulders", "SELL"

    if len(troughs) >= 2 and len(peaks) >= 1:
        t1_v, t2_v = p[troughs[-2]], p[troughs[-1]]
        peak_v = p[peaks[-1]]

        # Double Bottom
        if abs(t1_v - t2_v) / t1_v < 0.003 and last > peak_v:
            return "Double Bottom", "BUY"

        # Inverted H&S
        if len(troughs) >= 3:
            left, head, right = p[troughs[-3]], p[troughs[-2]], p[troughs[-1]]
            if head < left and head < right and abs(left - right) / left < 0.005:
                if last > peak_v:
                    return "Inverted H&S", "BUY"

    # Bull Flag
    recent_5 = p[-5:]
    prev_5   = p[-10:-5]
    if max(prev_5) > min(prev_5) * 1.003:  # strong prior move up
        if max(recent_5) < max(prev_5) and min(recent_5) > min(prev_5) * 0.999:
            return "Bull Flag", "BUY"

    # Bear Flag
    if min(prev_5) < max(prev_5) * 0.997:  # strong prior move down
        if min(recent_5) > min(prev_5) and max(recent_5) < max(prev_5) * 1.001:
            return "Bear Flag", "SELL"

    # Bullish Wedge (compression into support)
    if last > mid and (high - low) / last < 0.005:
        return "Bullish Wedge", "BUY"

    # Bearish Wedge
    if last < mid and (high - low) / last < 0.005:
        return "Bearish Wedge", "SELL"

    return None, None

# ─── LIQUIDITY SWEEP DETECTION ────────────────────────────
def detect_liquidity_sweep(prices):
    """Detect stop hunt / fake breakout pattern."""
    if len(prices) < 15:
        return None

    recent = prices[-5:]
    lookback = prices[-15:-5]

    prev_high = max(lookback)
    prev_low  = min(lookback)
    last = prices[-1]
    prev = prices[-2]

    # Swept high then reversed down
    if max(recent[:-1]) > prev_high and last < prev_high and last < prev:
        return "SELL"  # liquidity sweep above = sell

    # Swept low then reversed up
    if min(recent[:-1]) < prev_low and last > prev_low and last > prev:
        return "BUY"  # liquidity sweep below = buy

    return None

# ─── STRUCTURE ANALYSIS ───────────────────────────────────
def get_structure(prices):
    if len(prices) < 10:
        return "UNKNOWN"
    recent = prices[-10:]
    highs  = [recent[i] for i in range(1, len(recent)-1) if recent[i] > recent[i-1] and recent[i] > recent[i+1]]
    lows   = [recent[i] for i in range(1, len(recent)-1) if recent[i] < recent[i-1] and recent[i] < recent[i+1]]
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
            return "BULLISH"
        if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
            return "BEARISH"
    return "RANGING"

# ─── SIGNAL RATING ────────────────────────────────────────
def rate_signal(factors):
    """Rate signal as STRONG or MID based on confluence factors."""
    score = sum(factors.values())
    total = len(factors)
    pct   = score / total
    if pct >= 0.75:
        return "STRONG 🔥"
    elif pct >= 0.5:
        return "MID ⚡"
    return None  # Too weak — don't send

# ─── MAIN SIGNAL ENGINE ───────────────────────────────────
def analyze(prices, inst):
    if len(prices) < 12:
        return {"signal": "WAIT", "reason": "Building history..."}

    last     = prices[-1]
    prev     = prices[-2]
    momentum = last - prev
    recent   = prices[-12:]
    high     = max(recent)
    low      = min(recent)
    rng      = high - low
    rng_pct  = (rng / last) * 100
    dec      = inst["decimals"]
    sl       = inst["sl_dist"]

    # ── Volatility filter ──
    min_range = 0.03 if inst["id"] == "EURUSD" else 0.10
    if rng_pct < min_range:
        return {"signal": "WAIT", "reason": "Consolidating — no volatility"}

    # ── Spike filter ──
    spike_thresh = inst["sl_dist"] * 1.8
    if abs(momentum) > spike_thresh:
        return {"signal": "WAIT", "reason": "Post-spike — wait for structure"}

    # ── Structure ──
    structure = get_structure(prices)

    # ── Liquidity sweep ──
    liq_sweep = detect_liquidity_sweep(prices)

    # ── Chart pattern ──
    pattern, pattern_dir = detect_pattern(prices)

    # ── Basic SMC structure ──
    mid      = (high + low) / 2
    h_recent = recent[-4:]
    h_old    = recent[:4]
    bull_struct = h_recent[-1] > h_old[-1] and last > mid
    bear_struct = h_recent[-1] < h_old[-1] and last < mid
    bull_mom = momentum > 0 and (last - prices[-6]) > 0
    bear_mom = momentum < 0 and (last - prices[-6]) < 0

    prev_high = max(prices[-15:-3]) if len(prices) >= 15 else high
    prev_low  = min(prices[-15:-3]) if len(prices) >= 15 else low
    demand_retest = last <= prev_low * 1.0008 and bull_mom
    supply_retest = last >= prev_high * 0.9992 and bear_mom

    # ── Determine direction ──
    buy_signal  = False
    sell_signal = False
    reasons     = []

    if liq_sweep == "BUY":
        buy_signal = True
        reasons.append("Liquidity sweep reversal")
    elif liq_sweep == "SELL":
        sell_signal = True
        reasons.append("Liquidity sweep reversal")

    if pattern_dir == "BUY":
        buy_signal = True
        reasons.append(f"{pattern} pattern")
    elif pattern_dir == "SELL":
        sell_signal = True
        reasons.append(f"{pattern} pattern")

    if bull_struct and bull_mom and demand_retest:
        buy_signal = True
        reasons.append("HH/HL + demand retest")

    if bear_struct and bear_mom and supply_retest:
        sell_signal = True
        reasons.append("LH/LL + supply retest")

    if not buy_signal and not sell_signal:
        return {"signal": "WAIT", "reason": "No confluence setup"}

    # ── Conflict check ──
    if buy_signal and sell_signal:
        return {"signal": "WAIT", "reason": "Conflicting signals"}

    direction = "BUY" if buy_signal else "SELL"

    # ── Structure alignment check ──
    struct_aligned = (
        (direction == "BUY"  and structure in ("BULLISH", "UNKNOWN")) or
        (direction == "SELL" and structure in ("BEARISH", "UNKNOWN"))
    )

    # ── Rate signal ──
    factors = {
        "liquidity_sweep":  liq_sweep is not None,
        "pattern":          pattern is not None,
        "structure":        (bull_struct if direction=="BUY" else bear_struct),
        "momentum":         (bull_mom if direction=="BUY" else bear_mom),
        "retest":           (demand_retest if direction=="BUY" else supply_retest),
        "struct_aligned":   struct_aligned,
    }
    rating = rate_signal(factors)
    if not rating:
        return {"signal": "WAIT", "reason": "Setup too weak — skipping"}

    # ── Build signal ──
    tp1_mult = 2.0
    tp2_mult = 3.5
    if "STRONG" in rating:
        tp2_mult = 4.5

    if direction == "BUY":
        return {
            "signal":  "BUY",
            "rating":  rating,
            "entry":   round(last, dec),
            "sl":      round(last - sl, dec),
            "tp1":     round(last + sl * tp1_mult, dec),
            "tp2":     round(last + sl * tp2_mult, dec),
            "rr":      f"1:{tp1_mult}",
            "reason":  " + ".join(reasons),
            "structure": structure,
        }
    else:
        return {
            "signal":  "SELL",
            "rating":  rating,
            "entry":   round(last, dec),
            "sl":      round(last + sl, dec),
            "tp1":     round(last - sl * tp1_mult, dec),
            "tp2":     round(last - sl * tp2_mult, dec),
            "rr":      f"1:{tp1_mult}",
            "reason":  " + ".join(reasons),
            "structure": structure,
        }

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

last_update_id = 0
paused_markets = set()  # instruments currently paused by user

def check_telegram_commands():
    """Poll Telegram for commands every scan."""
    global last_update_id
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?timeout=1&offset={last_update_id + 1}&limit=10"
        r = requests.get(url, timeout=6)
        updates = r.json().get("result", [])
        for u in updates:
            last_update_id = u.get("update_id", last_update_id)
            msg = u.get("message", {}).get("text", "").strip().lower()
            if msg in ("/status", "status"):
                send_status()
            elif msg in ("/help", "help"):
                send_telegram(
                    "📋 *SMC Bot Commands*\n\n"
                    "`status` — Live bot status\n"
                    "`markets` — Show active/paused markets\n"
                    "`pause XAUUSD` — Pause Gold signals\n"
                    "`pause NAS100` — Pause Nasdaq signals\n"
                    "`pause EURUSD` — Pause EUR/USD signals\n"
                    "`pause USOUSD` — Pause Oil signals\n"
                    "`pause all` — Pause all markets\n"
                    "`resume XAUUSD` — Resume Gold signals\n"
                    "`resume all` — Resume all markets\n"
                    "`help` — Show this menu\n\n"
                    "_Signals are sent automatically._"
                )
            elif msg.startswith("pause"):
                parts = msg.split()
                target = parts[1].upper() if len(parts) > 1 else "ALL"
                if target == "ALL":
                    for i in INSTRUMENTS:
                        paused_markets.add(i["id"])
                    send_telegram("⏸ *All markets paused.*\nSend `resume all` to restart.")
                else:
                    valid = [i["id"] for i in INSTRUMENTS]
                    if target in valid:
                        paused_markets.add(target)
                        send_telegram(f"⏸ *{target}* signals paused.\nSend `resume {target}` to restart.")
                    else:
                        send_telegram(f"❓ Unknown market: `{target}`\nValid: `XAUUSD NAS100 EURUSD USOUSD`")
            elif msg.startswith("resume"):
                parts = msg.split()
                target = parts[1].upper() if len(parts) > 1 else "ALL"
                if target == "ALL":
                    paused_markets.clear()
                    send_telegram("▶️ *All markets resumed.*")
                else:
                    paused_markets.discard(target)
                    send_telegram(f"▶️ *{target}* signals resumed.")
            elif msg in ("/markets", "markets"):
                lines = []
                for i in INSTRUMENTS:
                    status = "⏸ PAUSED" if i["id"] in paused_markets else "✅ Active"
                    h = price_history[i["id"]]
                    price = round(h[-1], i["decimals"]) if h else "N/A"
                    lines.append(f"{status} — `{i['id']}` @ `{price}`")
                send_telegram("📊 *Market Status*\n\n" + "\n".join(lines))
    except Exception as e:
        log.warning(f"Command check error: {e}")

def send_status():
    now = datetime.now(timezone.utc)
    uptime = now - stats["start_time"]
    hours, rem = divmod(int(uptime.total_seconds()), 3600)
    mins = rem // 60
    last_scan_str = stats["last_scan"].strftime("%H:%M:%S UTC") if stats["last_scan"] else "N/A"
    last_sig_str  = stats["last_signal_sent"].strftime("%H:%M UTC") if stats["last_signal_sent"] else "None today"

    active_session = get_active_session(now)

    msg = (
        f"✅ *Bot Online*\n"
        f"🕐 Time: `{now.strftime('%H:%M:%S UTC')}`\n"
        f"⏱ Uptime: `{hours}h {mins}m`\n"
        f"━━━━━━━━━━━━━━\n"
        f"📊 Markets: `XAUUSD | NAS100 | EURUSD | USOUSD`\n"
        f"🔄 Status: `Monitoring`\n"
        f"🕵️ Last scan: `{last_scan_str}`\n"
        f"📡 Scan interval: `{SCAN_INTERVAL}s`\n"
        f"━━━━━━━━━━━━━━\n"
        f"🌍 Session: `{active_session}`\n"
        f"📨 Signals today: `{stats['signals_today']}`\n"
        f"⏰ Last signal: `{last_sig_str}`\n"
    )
    send_telegram(msg)

def get_active_session(now):
    hour = now.hour
    sessions = []
    if 7 <= hour < 16:  sessions.append("London 🇬🇧")
    if 12 <= hour < 21: sessions.append("New York 🇺🇸")
    if 0 <= hour < 9:   sessions.append("Tokyo 🇯🇵")
    if hour >= 22 or hour < 7: sessions.append("Sydney 🇦🇺")
    return " + ".join(sessions) if sessions else "Off-hours"

# ─── SESSION ANNOUNCEMENTS ────────────────────────────────
SESSION_MSGS = {
    "London_open":   ("🇬🇧 *London Session Open*\n`07:00 UTC` — Prime trading window begins.\nGold and indices most active. Watch for setups.", 7),
    "NewYork_open":  ("🇺🇸 *New York Session Open*\n`12:00 UTC` — High volatility period.\nBest overlap with London for NAS100 + XAUUSD.", 12),
    "London_close":  ("🇬🇧 *London Session Closing*\n`16:00 UTC` — Liquidity dropping.\nBe cautious with new entries.", 16),
    "NewYork_close": ("🇺🇸 *New York Session Closing*\n`21:00 UTC` — Markets winding down.\nAvoid new M1 entries.", 21),
    "Asian_open":    ("🌏 *Asian Session Active*\n`00:00 UTC` — Lower liquidity.\nGold may drift — wait for London for cleaner setups.", 0),
}

def check_session_announcements():
    now  = datetime.now(timezone.utc)
    hour = now.hour
    minute = now.minute
    if minute > 5:
        return
    for key, (msg, h) in SESSION_MSGS.items():
        day_key = f"{key}_{now.date()}"
        if hour == h and day_key not in announced_sessions:
            send_telegram(msg)
            announced_sessions.add(day_key)
            # Clean old keys
            if len(announced_sessions) > 20:
                oldest = list(announced_sessions)[0]
                announced_sessions.discard(oldest)

# ─── HEARTBEAT (every 20 mins if no signal) ───────────────
def check_heartbeat():
    now = datetime.now(timezone.utc)
    last_hb = stats["last_heartbeat"]
    last_sig = stats["last_signal_sent"]

    # Check if 20 mins passed since last heartbeat or signal
    ref_time = last_sig if last_sig else stats["start_time"]
    if last_hb and (now - last_hb).total_seconds() < 1200:
        return
    if (now - ref_time).total_seconds() < 1200:
        return

    session = get_active_session(now)
    prices_str = ""
    for inst in INSTRUMENTS:
        h = price_history[inst["id"]]
        if h:
            prices_str += f"`{inst['id']}: {round(h[-1], inst['decimals'])}` "

    msg = (
        f"💓 *Bot Heartbeat*\n"
        f"`{now.strftime('%H:%M UTC')}` — No signals in last 20 mins\n"
        f"Session: `{session}`\n"
        f"Prices: {prices_str}\n"
        f"_Monitoring continues..._"
    )
    send_telegram(msg)
    stats["last_heartbeat"] = now


# ─── LOT SIZE + PROFIT ESTIMATE ───────────────────────────
def get_lot_recommendation(rating, inst_id):
    """Recommend lot size based on signal strength."""
    if "STRONG" in rating:
        lots = 0.03
    elif "MID" in rating:
        lots = 0.02
    else:
        lots = 0.01
    return lots

def estimate_profit(inst_id, entry, tp1, tp2, sl, lots):
    """Estimate profit at TP1/TP2 and loss if SL hits."""
    try:
        dist_tp1 = abs(float(tp1) - float(entry))
        dist_tp2 = abs(float(tp2) - float(entry))
        dist_sl  = abs(float(sl)  - float(entry))

        if inst_id == "XAUUSD":
            pip_val      = 100 * lots
            profit_tp1   = round(dist_tp1 * pip_val, 2)
            profit_tp2   = round(dist_tp2 * pip_val, 2)
            loss_sl      = round(dist_sl  * pip_val, 2)
        elif inst_id == "NAS100":
            pip_val      = 100 * lots
            profit_tp1   = round(dist_tp1 * pip_val, 2)
            profit_tp2   = round(dist_tp2 * pip_val, 2)
            loss_sl      = round(dist_sl  * pip_val, 2)
        elif inst_id == "EURUSD":
            profit_tp1   = round(100000 * lots * dist_tp1, 2)
            profit_tp2   = round(100000 * lots * dist_tp2, 2)
            loss_sl      = round(100000 * lots * dist_sl,  2)
        elif inst_id == "USOUSD":
            pip_val      = 1000 * lots
            profit_tp1   = round(dist_tp1 * pip_val, 2)
            profit_tp2   = round(dist_tp2 * pip_val, 2)
            loss_sl      = round(dist_sl  * pip_val, 2)
        else:
            profit_tp1 = profit_tp2 = loss_sl = 0.0

        return profit_tp1, profit_tp2, loss_sl
    except:
        return 0.0, 0.0, 0.0

# ─── FORMAT SIGNAL MESSAGE ────────────────────────────────
def format_signal(inst, analysis):
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    sig = analysis["signal"]
    emoji = "🟢" if sig == "BUY" else "🔴"
    rating = analysis.get("rating", "MID ⚡")

    rating = analysis.get("rating", "MID ⚡")
    lots = get_lot_recommendation(rating, inst["id"])
    profit_tp1, profit_tp2, loss_sl = estimate_profit(
        inst["id"], analysis["entry"], analysis["tp1"], analysis["tp2"], analysis["sl"], lots
    )

    return (
        f"{emoji} *{sig} — {inst['id']}* ({inst['label']})\n"
        f"⭐ Rating: *{rating}*\n"
        f"⏰ `{now}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"📍 Entry:  *{analysis['entry']}*\n"
        f"🛑 SL:     `{analysis['sl']}`\n"
        f"🎯 TP1:    `{analysis['tp1']}`\n"
        f"🎯 TP2:    `{analysis['tp2']}`\n"
        f"📊 RR:     `{analysis['rr']}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"💼 Lot Size:  *{lots}*\n"
        f"💰 Est. Profit TP1: *~${profit_tp1}*\n"
        f"💰 Est. Profit TP2: *~${profit_tp2}*\n"
        f"🔻 Est. Loss if SL: *~-${loss_sl}*\n"
        f"━━━━━━━━━━━━━━\n"
        f"🧠 _{analysis['reason']}_\n"
        f"📈 Structure: `{analysis.get('structure','N/A')}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"Copy TP1: `{analysis['tp1']}`\n"
        f"Copy TP2: `{analysis['tp2']}`\n"
        f"⚠️ _Confirm on chart before trading_"
    )

# ─── MAIN SCAN ────────────────────────────────────────────
def scan():
    stats["last_scan"] = datetime.now(timezone.utc)
    for inst in INSTRUMENTS:
        try:
            if inst["id"] in paused_markets:
                log.info(f"{inst['id']} | PAUSED — skipping")
                continue
            price = get_price(inst)
            if price and price > 0:
                price_history[inst["id"]].append(price)
                if len(price_history[inst["id"]]) > 150:
                    price_history[inst["id"]].pop(0)

            analysis = analyze(price_history[inst["id"]], inst)
            sig      = analysis["signal"]
            prev_sig = last_signals[inst["id"]]

            log.info(f"{inst['id']} | {price} | {sig} | {analysis.get('reason','')}")

            if sig != "WAIT" and sig != prev_sig:
                last_signals[inst["id"]]    = sig
                last_signal_time[inst["id"]]= datetime.now(timezone.utc)
                msg = format_signal(inst, analysis)
                send_telegram(msg)
                stats["signals_today"] += 1
                stats["last_signal_sent"] = datetime.now(timezone.utc)
                stats["last_heartbeat"]   = datetime.now(timezone.utc)

            elif sig == "WAIT" and prev_sig in ("BUY", "SELL"):
                last_signals[inst["id"]] = "WAIT"
                send_telegram(f"⏸ *{inst['id']}* signal cleared — now `WAIT`")

        except Exception as e:
            log.error(f"Error scanning {inst['id']}: {e}")

# ─── RESET DAILY STATS ────────────────────────────────────
last_reset_day = None
def reset_daily_stats():
    global last_reset_day
    today = datetime.now(timezone.utc).date()
    if last_reset_day != today:
        stats["signals_today"] = 0
        last_reset_day = today
        announced_sessions.clear()

# ─── MAIN ─────────────────────────────────────────────────
def main():
    log.info("SMC Signal Bot v2 — Starting")
    send_telegram(
        "✅ *SMC Signal Bot v2 Online*\n"
        f"📊 Monitoring: `XAUUSD | NAS100 | EURUSD | USOUSD`\n"
        f"🔄 Scan every: `{SCAN_INTERVAL}s`\n"
        f"⭐ Signal ratings: `STRONG 🔥 / MID ⚡`\n"
        f"💓 Heartbeat: every `20 mins` if no signal\n"
        f"📡 Send `status` anytime for live update\n"
        f"_Signals will appear here automatically._"
    )
    tick = 0
    while True:
        try:
            reset_daily_stats()
            scan()
            check_session_announcements()
            check_heartbeat()
            check_telegram_commands()
            tick += 1
        except Exception as e:
            log.error(f"Main loop error: {e}")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    main()
