import os
import time
import requests
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ─── CONFIG (set these in Railway environment variables) ───
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", "60"))  # seconds

# ─── INSTRUMENTS ───────────────────────────────────────────
# TwelveData free tier supports: XAU/USD, EUR/USD
# NAS100 and Oil fall back to Yahoo Finance automatically
INSTRUMENTS = [
    {"id": "XAUUSD", "label": "Gold",    "symbol": "XAU/USD",  "decimals": 2, "sl_dist": 3.5,   "td": True},
    {"id": "NAS100", "label": "Nasdaq",  "symbol": None,       "decimals": 1, "sl_dist": 15.0,  "td": False},
    {"id": "USOUSD", "label": "WTI Oil", "symbol": None,       "decimals": 2, "sl_dist": 0.8,   "td": False},
    {"id": "EURUSD", "label": "EUR/USD", "symbol": "EUR/USD",  "decimals": 5, "sl_dist": 0.0012,"td": True},
]

# ─── STATE ─────────────────────────────────────────────────
price_history = {i["id"]: [] for i in INSTRUMENTS}
last_signals  = {i["id"]: None for i in INSTRUMENTS}

# ─── PRICE FETCHING ────────────────────────────────────────
def fetch_price_twelvedata(symbol):
    """Fetch latest price from TwelveData (free tier: 800 req/day)."""
    try:
        url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TWELVEDATA_API_KEY}"
        r = requests.get(url, timeout=10)
        data = r.json()
        if "price" in data:
            return float(data["price"])
        log.warning(f"TwelveData error for {symbol}: {data}")
    except Exception as e:
        log.error(f"fetch_price error {symbol}: {e}")
    return None

def fetch_price_fallback_eurusd():
    """Fallback FX rate from Frankfurter."""
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=EUR&to=USD", timeout=10)
        return float(r.json()["rates"]["USD"])
    except:
        return None

def fetch_price_fallback_gold():
    """Fallback gold price from Yahoo Finance."""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/GC%3DF?interval=1m&range=5m"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        return float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except:
        return None

def fetch_price_fallback_oil():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/CL%3DF?interval=1m&range=5m"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        return float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except:
        return None

def fetch_price_fallback_nas():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC?interval=1m&range=5m"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        return float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except:
        return None

def get_price(inst):
    """Try TwelveData first (only for supported symbols), fall back to free sources."""
    if TWELVEDATA_API_KEY and inst.get("td") and inst.get("symbol"):
        price = fetch_price_twelvedata(inst["symbol"])
        if price:
            return price
    # Fallbacks
    if inst["id"] == "EURUSD":
        return fetch_price_fallback_eurusd()
    elif inst["id"] == "XAUUSD":
        return fetch_price_fallback_gold()
    elif inst["id"] == "USOUSD":
        return fetch_price_fallback_oil()
    elif inst["id"] == "NAS100":
        return fetch_price_fallback_nas()
    return None

# ─── SMC SIGNAL ENGINE ─────────────────────────────────────
def analyze(prices, inst):
    if len(prices) < 8:
        return {"signal": "WAIT", "reason": "Building price history..."}

    recent   = prices[-8:]
    last     = recent[-1]
    prev     = recent[-2]
    momentum = last - prev
    high     = max(recent)
    low      = min(recent)
    rng      = high - low
    rng_pct  = (rng / last) * 100

    sl   = inst["sl_dist"]
    tp1  = sl * 2
    tp2  = sl * 4
    dec  = inst["decimals"]

    # Spike check
    spike_thresh = 0.0015 if inst["id"] == "EURUSD" else (inst["sl_dist"] * 1.5)
    if abs(momentum) > spike_thresh:
        return {"signal": "WAIT", "reason": "Post-spike — no trade"}

    # Choppy check
    choppy_thresh = 0.04 if inst["id"] == "EURUSD" else 0.12
    if rng_pct < choppy_thresh:
        return {"signal": "WAIT", "reason": "Consolidating — no structure"}

    # Structure
    mid      = (high + low) / 2
    h_recent = recent[-4:]
    h_old    = recent[:4]

    hh_hl = h_recent[-1] > h_old[-1] and last > mid  # bullish structure
    lh_ll = h_recent[-1] < h_old[-1] and last < mid  # bearish structure

    bull_mom = momentum > 0 and (last - recent[0]) > 0
    bear_mom = momentum < 0 and (last - recent[0]) < 0

    prev_high = max(prices[-10:-2]) if len(prices) >= 10 else high
    prev_low  = min(prices[-10:-2]) if len(prices) >= 10 else low

    demand_retest = last <= prev_low * 1.0005 and bull_mom
    supply_retest = last >= prev_high * 0.9995 and bear_mom

    if hh_hl and bull_mom and demand_retest:
        return {
            "signal": "BUY",
            "entry":  round(last, dec),
            "sl":     round(last - sl, dec),
            "tp1":    round(last + tp1, dec),
            "tp2":    round(last + tp2, dec),
            "reason": "HH/HL structure + demand retest"
        }

    if lh_ll and bear_mom and supply_retest:
        return {
            "signal": "SELL",
            "entry":  round(last, dec),
            "sl":     round(last + sl, dec),
            "tp1":    round(last - tp1, dec),
            "tp2":    round(last - tp2, dec),
            "reason": "LH/LL structure + supply retest"
        }

    return {"signal": "WAIT", "reason": "No high-probability setup"}

# ─── TELEGRAM ──────────────────────────────────────────────
def send_telegram(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
        r = requests.post(url, json=payload, timeout=10)
        if not r.json().get("ok"):
            log.warning(f"Telegram send failed: {r.text}")
    except Exception as e:
        log.error(f"Telegram error: {e}")

def format_signal_message(inst, analysis, price):
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    if analysis["signal"] == "BUY":
        emoji = "🟢"
    elif analysis["signal"] == "SELL":
        emoji = "🔴"
    else:
        emoji = "🟡"

    if analysis["signal"] in ("BUY", "SELL"):
        return (
            f"{emoji} *{analysis['signal']} — {inst['id']}* ({inst['label']})\n"
            f"⏰ `{now}`\n"
            f"━━━━━━━━━━━━━━\n"
            f"📍 Entry:  `{analysis['entry']}`\n"
            f"🛑 SL:     `{analysis['sl']}`\n"
            f"🎯 TP1:    `{analysis['tp1']}`\n"
            f"🎯 TP2:    `{analysis['tp2']}`\n"
            f"━━━━━━━━━━━━━━\n"
            f"📊 _{analysis['reason']}_\n"
            f"⚠️ _Confirm on your chart before trading_"
        )
    return None  # Don't send WAIT signals to Telegram (too noisy)

# ─── MAIN LOOP ─────────────────────────────────────────────
def scan():
    log.info("Running scan...")
    for inst in INSTRUMENTS:
        price = get_price(inst)
        if price and price > 0:
            price_history[inst["id"]].append(price)
            if len(price_history[inst["id"]]) > 100:
                price_history[inst["id"]].pop(0)

        history = price_history[inst["id"]]
        analysis = analyze(history, inst)
        sig = analysis["signal"]
        prev_sig = last_signals[inst["id"]]

        log.info(f"{inst['id']} | Price: {price} | Signal: {sig} | {analysis.get('reason','')}")

        # Only notify on NEW signal (not repeat WAITs)
        if sig != "WAIT" and sig != prev_sig:
            last_signals[inst["id"]] = sig
            msg = format_signal_message(inst, analysis, price)
            if msg:
                send_telegram(msg)
                log.info(f"Signal sent to Telegram: {inst['id']} {sig}")

        elif sig == "WAIT" and prev_sig in ("BUY", "SELL"):
            # Signal cleared — optionally notify
            last_signals[inst["id"]] = "WAIT"
            send_telegram(f"⏸ *{inst['id']}* signal cleared — `WAIT`")

def main():
    log.info("═══════════════════════════════════")
    log.info("  SMC Signal Bot — Starting up")
    log.info(f"  Instruments: {[i['id'] for i in INSTRUMENTS]}")
    log.info(f"  Scan interval: {SCAN_INTERVAL}s")
    log.info(f"  TwelveData: {'✓ configured' if TWELVEDATA_API_KEY else '✗ using fallback APIs'}")
    log.info(f"  Telegram: {'✓ configured' if TELEGRAM_TOKEN else '✗ NOT configured'}")
    log.info("═══════════════════════════════════")

    # Send startup message
    send_telegram(
        "✅ *SMC Signal Bot Online*\n"
        f"Monitoring: `XAUUSD | NAS100 | USOUSD | EURUSD`\n"
        f"Scan every `{SCAN_INTERVAL}s`\n"
        "_Signals will appear here automatically._"
    )

    while True:
        try:
            scan()
        except Exception as e:
            log.error(f"Scan error: {e}")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    main()

