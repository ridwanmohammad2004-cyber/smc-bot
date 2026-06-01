# SMC Signal Bot — Railway Deployment Guide

## What this does
Runs 24/7 on Railway, fetches live prices for XAUUSD, NAS100, USOUSD, EURUSD
and sends BUY/SELL signals directly to your Telegram.

---

## Step 1 — Set up Telegram Bot (2 mins)

1. Open Telegram → search **@BotFather**
2. Send: `/newbot`
3. Give it a name e.g. `SMC Signals` and username e.g. `mysmc_bot`
4. Copy the **bot token** (looks like: `7123456789:AAFxxx...`)
5. Now search **@userinfobot** → send any message → copy your **Chat ID** (a number)

---

## Step 2 — Get TwelveData API Key (optional but recommended)

1. Go to https://twelvedata.com → Sign up free
2. Go to Dashboard → copy your API key
3. Free tier = 800 requests/day (enough for 30s scans on 4 instruments)

---

## Step 3 — Deploy to Railway (5 mins)

1. Go to https://railway.app → Sign up with GitHub
2. Click **New Project** → **Deploy from GitHub repo**
   - OR click **New Project** → **Empty Project** → drag and drop the folder
3. Once project is created, click your service → **Variables** tab
4. Add these environment variables:

| Variable | Value |
|----------|-------|
| `TELEGRAM_TOKEN` | your bot token from BotFather |
| `TELEGRAM_CHAT_ID` | your chat ID from userinfobot |
| `TWELVEDATA_API_KEY` | your TwelveData key (optional) |
| `SCAN_INTERVAL` | `60` (seconds between scans) |

5. Railway will auto-deploy. Check **Logs** tab to see it running.

---

## Step 4 — Verify it works

- Check the **Logs** tab in Railway — you should see scan output every 60s
- Your Telegram should receive: "✅ SMC Signal Bot Online"
- When a signal fires you'll get a message like:

```
🔴 SELL — XAUUSD (Gold)
⏰ 14:32 UTC
━━━━━━━━━━━━━
📍 Entry:  3312.50
🛑 SL:     3316.00
🎯 TP1:    3305.50
🎯 TP2:    3298.50
━━━━━━━━━━━━━
📊 LH/LL structure + supply retest
⚠️ Confirm on your chart before trading
```

---

## Deploying via GitHub (easiest method)

1. Create a free GitHub account
2. Create a new repository called `smc-bot`
3. Upload all 4 files: `bot.py`, `requirements.txt`, `Procfile`, `README.md`
4. In Railway → New Project → Deploy from GitHub → select `smc-bot`
5. Add environment variables → done

---

## Files in this package
- `bot.py` — main signal bot
- `requirements.txt` — Python dependencies
- `Procfile` — tells Railway how to run it
- `README.md` — this guide
