import os
import time
import json
import threading
import requests
import logging
from datetime import datetime, timezone

try:
    import websocket  # websocket-client
except ImportError:
    websocket = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
ANALYZE_INTERVAL   = int(os.environ.get("ANALYZE_INTERVAL", "5"))   # how often to analyze (seconds)
USE_WEBSOCKET      = os.environ.get("USE_WEBSOCKET", "true").lower() == "true"

# ─── INSTRUMENTS (PU Prime Islamic Standard) ──────────────
INSTRUMENTS = [
    {"id": "XAUUSD", "label": "Gold",    "symbol": "XAU/USD",  "decimals": 2,  "sl_dist": 3.5,   "priority": 1},
    {"id": "NAS100", "label": "Nasdaq",  "symbol": "NDX",      "decimals": 1,  "sl_dist": 20.0,  "priority": 2},
    {"id": "EURUSD", "label": "EUR/USD", "symbol": "EUR/USD",  "decimals": 5,  "sl_dist": 0.0015,"priority": 3},
    {"id": "USOUSD", "label": "WTI Oil", "symbol": "USO",      "decimals": 2,  "sl_dist": 0.8,   "priority": 4},
]
SYMBOL_TO_ID = {i["symbol"]: i["id"] for i in INSTRUMENTS}

# ─── STATE ────────────────────────────────────────────────
price_history    = {i["id"]: [] for i in INSTRUMENTS}
latest_price     = {i["id"]: None for i in INSTRUMENTS}
last_signals     = {i["id"]: None for i in INSTRUMENTS}
last_signal_time = {i["id"]: None for i in INSTRUMENTS}
stats = {
    "signals_today": 0,
    "last_scan": None,
    "start_time": datetime.now(timezone.utc),
    "last_signal_sent": None,
    "last_heartbeat": None,
    "ws_connected": False,
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
                inst_id = SYMBOL_TO_ID[symbol]
                latest_price[inst_id] = float(price)
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
    symbols = ",".join(i["symbol"] for i in INSTRUMENTS)
    sub = {"action": "subscribe", "params": {"symbols": symbols}}
    ws.send(json.dumps(sub))
    log.info(f"Subscribed to: {symbols}")

def run_websocket():
    """Run TwelveData WebSocket connection with auto-reconnect."""
    if websocket is None:
        log.error("websocket-client not installed — falling back to REST")
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

def rest_poll_loop():
    """Fallback REST polling if WebSocket unavailable."""
    while True:
        for inst in INSTRUMENTS:
            if inst["id"] in paused_markets:
                continue
            price = fetch_twelvedata(inst["symbol"])
            if price:
                latest_price[inst["id"]] = price
        time.sleep(10)

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
    recent = prices[-5:]
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
    pct = score / len(factors)
    if pct >= 0.75:
        return "STRONG 🔥"
    elif pct >= 0.5:
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

    min_range = 0.03 if inst["id"] == "EURUSD" else 0.10
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
    if liq_sweep == "BUY":  buy_signal = True;  reasons.append("Liquidity sweep reversal")
    elif liq_sweep == "SELL": sell_signal = True; reasons.append("Liquidity sweep reversal")
    if pattern_dir == "BUY":  buy_signal = True;  reasons.append(f"{pattern} pattern")
    elif pattern_dir == "SELL": sell_signal = True; reasons.append(f"{pattern} pattern")
    if bull_struct and bull_mom and demand_retest:
        buy_signal = True; reasons.append("HH/HL + demand retest")
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
        "pattern": pattern is not None,
        "structure": (bull_struct if direction=="BUY" else bear_struct),
        "momentum": (bull_mom if direction=="BUY" else bear_mom),
        "retest": (demand_retest if direction=="BUY" else supply_retest),
        "struct_aligned": struct_aligned,
    }
    rating = rate_signal(factors)
    if not rating:
        return {"signal": "WAIT", "reason": "Setup too weak — skipping"}

    tp1_mult, tp2_mult = 2.0, 3.5
    if "STRONG" in rating:
        tp2_mult = 4.5

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
def get_lot_recommendation(rating):
    if "STRONG" in rating: return 0.03
    elif "MID" in rating:  return 0.02
    return 0.01

def estimate_profit(inst_id, entry, tp1, tp2, sl, lots):
    try:
        d_tp1 = abs(float(tp1) - float(entry))
        d_tp2 = abs(float(tp2) - float(entry))
        d_sl  = abs(float(sl)  - float(entry))
        if inst_id in ("XAUUSD", "NAS100"):
            pv = 100 * lots
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
    emoji = "🟢" if a["signal"] == "BUY" else "🔴"
    rating = a.get("rating", "MID ⚡")
    lots = get_lot_recommendation(rating)
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
        f"⚠️ _Confirm on chart before trading_"
    )

# ─── COMMANDS ─────────────────────────────────────────────
def check_telegram_commands():
    global last_update_id
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?timeout=1&offset={last_update_id+1}&limit=10"
        r = requests.get(url, timeout=6)
        for u in r.json().get("result", []):
            last_update_id = u.get("update_id", last_update_id)
            msg = u.get("message", {}).get("text", "").strip().lower()
            if msg in ("/status", "status"):
                send_status()
            elif msg in ("/help", "help"):
                send_telegram(
                    "📋 *SMC Bot Commands*\n\n"
                    "`status` — Live bot status\n"
                    "`markets` — Active/paused markets\n"
                    "`pause XAUUSD` — Pause a market\n"
                    "`pause all` — Pause everything\n"
                    "`resume XAUUSD` — Resume a market\n"
                    "`resume all` — Resume all\n"
                    "`help` — This menu"
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
    except Exception as e:
        log.warning(f"Command error: {e}")

def get_active_session(now):
    h = now.hour
    s = []
    if 7 <= h < 16:  s.append("London 🇬🇧")
    if 12 <= h < 21: s.append("New York 🇺🇸")
    if 0 <= h < 9:   s.append("Tokyo 🇯🇵")
    if h >= 22 or h < 7: s.append("Sydney 🇦🇺")
    return " + ".join(s) if s else "Off-hours"

def send_status():
    now = datetime.now(timezone.utc)
    up = now - stats["start_time"]
    h, rem = divmod(int(up.total_seconds()), 3600)
    m = rem // 60
    ls = stats["last_scan"].strftime("%H:%M:%S UTC") if stats["last_scan"] else "N/A"
    lsig = stats["last_signal_sent"].strftime("%H:%M UTC") if stats["last_signal_sent"] else "None today"
    feed = "🟢 WebSocket Live" if stats["ws_connected"] else "🟡 REST Fallback"
    send_telegram(
        f"✅ *Bot Online*\n"
        f"🕐 Time: `{now.strftime('%H:%M:%S UTC')}`\n"
        f"⏱ Uptime: `{h}h {m}m`\n"
        f"📡 Feed: {feed}\n"
        f"━━━━━━━━━━━━━━\n"
        f"📊 Markets: `XAUUSD | NAS100 | EURUSD | USOUSD`\n"
        f"🔄 Status: `Monitoring`\n"
        f"🕵️ Last analysis: `{ls}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"🌍 Session: `{get_active_session(now)}`\n"
        f"📨 Signals today: `{stats['signals_today']}`\n"
        f"⏰ Last signal: `{lsig}`"
    )

SESSION_MSGS = {
    "London_open":   ("🇬🇧 *London Session Open*\n`07:00 UTC` — Prime window. Watch for setups.", 7),
    "NewYork_open":  ("🇺🇸 *New York Session Open*\n`12:00 UTC` — High volatility. Best overlap.", 12),
    "London_close":  ("🇬🇧 *London Closing*\n`16:00 UTC` — Liquidity dropping.", 16),
    "NewYork_close": ("🇺🇸 *New York Closing*\n`21:00 UTC` — Markets winding down.", 21),
    "Asian_open":    ("🌏 *Asian Session*\n`00:00 UTC` — Lower liquidity. Wait for London.", 0),
}

def check_session_announcements():
    now = datetime.now(timezone.utc)
    if now.minute > 5:
        return
    for key, (msg, h) in SESSION_MSGS.items():
        dk = f"{key}_{now.date()}"
        if now.hour == h and dk not in announced_sessions:
            send_telegram(msg)
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
        stats["signals_today"] = 0
        last_reset_day = today
        announced_sessions.clear()

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
                    # Only append if price changed (avoid duplicate flooding)
                    if not hist or hist[-1] != price:
                        hist.append(price)
                        if len(hist) > 150:
                            hist.pop(0)

                analysis = analyze(price_history[inst["id"]], inst)
                sig = analysis["signal"]
                prev_sig = last_signals[inst["id"]]
                log.info(f"{inst['id']} | {price} | {sig} | {analysis.get('reason','')}")

                if sig != "WAIT" and sig != prev_sig:
                    last_signals[inst["id"]] = sig
                    last_signal_time[inst["id"]] = datetime.now(timezone.utc)
                    send_telegram(format_signal(inst, analysis))
                    stats["signals_today"] += 1
                    stats["last_signal_sent"] = datetime.now(timezone.utc)
                    stats["last_heartbeat"] = datetime.now(timezone.utc)
                elif sig == "WAIT" and prev_sig in ("BUY", "SELL"):
                    last_signals[inst["id"]] = "WAIT"
                    send_telegram(
                        f"⏸ *{inst['id']}* signal cleared — now `WAIT`\n"
                        f"_Previous signal no longer valid. Don't enter if not already in trade._"
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
    log.info("SMC Signal Bot v3 (WebSocket) — Starting")
    feed_type = "WebSocket (real-time)" if (USE_WEBSOCKET and websocket) else "REST polling"
    send_telegram(
        "✅ *SMC Signal Bot v3 Online*\n"
        f"📊 Monitoring: `XAUUSD | NAS100 | EURUSD | USOUSD`\n"
        f"📡 Feed: `{feed_type}`\n"
        f"⚡ Analysis every: `{ANALYZE_INTERVAL}s`\n"
        f"⭐ Ratings: `STRONG 🔥 / MID ⚡`\n"
        f"💓 Heartbeat: every `20 mins` if no signal\n"
        f"📡 Send `status` anytime\n"
        f"_Signals appear here automatically._"
    )

    # Start price feed thread
    if USE_WEBSOCKET and websocket:
        t = threading.Thread(target=run_websocket, daemon=True)
        t.start()
        log.info("WebSocket thread started")
    else:
        t = threading.Thread(target=rest_poll_loop, daemon=True)
        t.start()
        log.info("REST polling thread started")

    # Give feed a moment to populate
    time.sleep(3)

    # Run analysis loop in main thread
    analysis_loop()

if __name__ == "__main__":
    main()
