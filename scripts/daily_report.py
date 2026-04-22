"""
Daily Market Report — Telegram Bot
Runs every morning at 07:00 Bangkok time via Claude Remote Trigger.

Sections:
  1. Starting message
  2. Top 20 Gainers (Binance 1h)
  3. Top 20 Losers  (Binance 1h)
  4. Watchlist — TV signal + CDC Action Zone (Crypto / Commodities / Stocks US / Stocks TH)
  5. AI Deep Analysis — TradingAgents + DeepSeek (AAPL, BTC, GOLD)
"""
from __future__ import annotations

import json
import os
import re
import site
import subprocess
import sys
import urllib.request

# ── Ensure src/ is on path ─────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# ── Config ─────────────────────────────────────────────────────────────────────
BOT  = "8720452318:AAGgh2WXUW6JFw_Z71eMUBZ0bi-n5eHnwuE"
CHAT = "5636156156"
BASE = "https://signal-tradingview-mcp.up.railway.app"
os.environ.setdefault("DEEPSEEK_API_KEY", "sk-9d9423f9ab86437197bbe96180d401e3")


# ── Helpers ────────────────────────────────────────────────────────────────────

def send(msg: str) -> bool:
    payload = json.dumps({"chat_id": CHAT, "text": msg, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{BOT}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        return json.loads(urllib.request.urlopen(req).read()).get("ok", False)
    except Exception as e:
        print("Send error:", e)
        return False


def fetch(url: str):
    try:
        return json.loads(urllib.request.urlopen(url, timeout=20).read())
    except Exception:
        return None


def sig_emoji(s: str) -> str:
    return {
        "BUY": "🟢", "STRONG_BUY": "🟢",
        "SELL": "🔴", "STRONG_SELL": "🔴",
        "NEUTRAL": "🟡",
    }.get(s or "", "⚪")


from datetime import datetime
today      = datetime.now().strftime("%d/%m/%Y")
today_date = datetime.now().strftime("%Y-%m-%d")


# ── STEP 1 — Starting message ──────────────────────────────────────────────────
send(f"🤖 <b>Daily Market Report</b>\n🗓 {today} | 07:00 BKK\n⏳ กำลังดึงข้อมูล รอสักครู่นะครับ...")
print("Step 1 done")


# ── STEP 2 — Top 20 Gainers ───────────────────────────────────────────────────
data = fetch(f"{BASE}/api/top-gainers?exchange=BINANCE&timeframe=1h&limit=20")
if data:
    lines = [f"📈 <b>TOP 20 GAINERS — BINANCE 1H</b>\n🗓 {today}"]
    for i, r in enumerate(data[:20], 1):
        sym = r["symbol"].replace("BINANCE:", "")
        lines.append(f"{i}. 🟢 {sym}  +{r['changePercent']}%")
    send("\n".join(lines))
print("Step 2 done")


# ── STEP 3 — Top 20 Losers ────────────────────────────────────────────────────
data = fetch(f"{BASE}/api/top-losers?exchange=BINANCE&timeframe=1h&limit=20")
if data:
    lines = ["📉 <b>TOP 20 LOSERS — BINANCE 1H</b>"]
    for i, r in enumerate(data[:20], 1):
        sym = r["symbol"].replace("BINANCE:", "")
        lines.append(f"{i}. 🔴 {sym}  {r['changePercent']}%")
    send("\n".join(lines))
print("Step 3 done")


# ── STEP 4 — Watchlist ────────────────────────────────────────────────────────
from tradingview_mcp.core.services.screener_service import analyze_coin
from tradingview_mcp.core.services.yahoo_finance_service import get_price
from tradingview_mcp.core.services.cdc_service import analyze_cdc

lines = ["🎯 <b>WATCHLIST — TECHNICAL SIGNALS</b>", ""]

# Crypto
lines.append("─── CRYPTO ───")
for sym, label in [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"), ("SOLUSDT", "SOL"), ("XRPUSDT", "XRP")]:
    try:
        d    = analyze_coin(sym, "binance", "1h")
        sig  = d.get("market_sentiment", {}).get("buy_sell_signal", "N/A")
        price = d.get("price_data", {}).get("current_price", 0)
        if not price:
            r2    = json.loads(urllib.request.urlopen(f"https://api.binance.com/api/v3/ticker/price?symbol={sym}", timeout=5).read())
            price = float(r2.get("price", 0))
        rsi   = d.get("rsi", {}).get("value", "N/A")
        rsi_s = f"{rsi:.1f}" if isinstance(rsi, float) else str(rsi)
        cdc   = analyze_cdc(sym, "binance", "1h", sig)
        lines.append(f"{sig_emoji(sig)} <b>{label}</b>  ${price:,.2f}  |  {sig}  |  RSI:{rsi_s}  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{label}</b>  N/A")

# Commodities
lines += ["", "─── COMMODITIES ───"]
for ticker, label, emoji in [("GC=F", "GOLD", "🥇"), ("SI=F", "SILVER", "🥈"), ("HG=F", "COPPER", "🥉"), ("CL=F", "WTI", "🛢")]:
    try:
        d   = get_price(ticker)
        cdc = analyze_cdc(ticker, "yahoo", "1D")
        lines.append(f"{emoji} <b>{label}</b>  ${d['price']:,.2f}  ({d['change_pct']:+.2f}%)  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{label}</b>  N/A")

# Stocks US
lines += ["", "─── STOCKS US ───"]
for sym in ["AAPL", "MSFT", "TSLA", "NVDA", "AMZN", "META", "GOOG", "PRCT", "TGLS", "IREN", "VT", "JEPQ"]:
    try:
        d     = analyze_coin(sym, "nasdaq", "1D")
        sig   = d.get("market_sentiment", {}).get("buy_sell_signal", "N/A")
        price = d.get("price_data", {}).get("current_price", 0)
        if not price:
            import yfinance as yf
            t = yf.Ticker(sym)
            price = t.fast_info.get("lastPrice", 0) or t.fast_info.get("previousClose", 0)
        rsi   = d.get("rsi", {}).get("value", "N/A")
        rsi_s = f"{rsi:.1f}" if isinstance(rsi, float) else str(rsi)
        cdc   = analyze_cdc(sym, "nasdaq", "1D", sig)
        lines.append(f"{sig_emoji(sig)} <b>{sym}</b>  ${price:,.2f}  |  {sig}  |  RSI:{rsi_s}  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{sym}</b>  N/A")

# Stocks TH
lines += ["", "─── STOCKS TH ───"]
for sym in ["SCB", "KTB", "BCPG"]:
    try:
        d     = analyze_coin(sym, "set", "1D")
        sig   = d.get("market_sentiment", {}).get("buy_sell_signal", "N/A")
        price = d.get("price_data", {}).get("current_price", 0)
        if not price:
            import yfinance as yf
            t = yf.Ticker(f"{sym}.BK")
            price = t.fast_info.get("lastPrice", 0) or t.fast_info.get("previousClose", 0)
        rsi   = d.get("rsi", {}).get("value", "N/A")
        rsi_s = f"{rsi:.1f}" if isinstance(rsi, float) else str(rsi)
        cdc   = analyze_cdc(sym, "set", "1D", sig)
        lines.append(f"{sig_emoji(sig)} <b>{sym}</b>  ฿{price:,.2f}  |  {sig}  |  RSI:{rsi_s}  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{sym}</b>  N/A")

send("\n".join(lines))
print("Step 4 done")


# ── STEP 5 — AI Deep Analysis (DeepSeek single-call, ~30s per symbol) ──────────


def deepseek_analyze(label: str, tv_sig: str, cdc_zone: str, combined: str, price_str: str) -> dict:
    """Single DeepSeek API call via openai client — ~8s, returns BUY/SELL/HOLD with reasoning."""
    from openai import OpenAI
    client = OpenAI(
        api_key=os.environ.get("DEEPSEEK_API_KEY", ""),
        base_url="https://api.deepseek.com/v1",
    )
    prompt = (
        f"You are an expert financial analyst. Analyze {label}.\n\n"
        f"Current signals:\n"
        f"- TradingView Signal: {tv_sig}\n"
        f"- CDC Action Zone: {cdc_zone}\n"
        f"- Combined Signal: {combined}\n"
        f"- Price: {price_str}\n\n"
        'Respond ONLY in this exact JSON (no markdown):\n'
        '{"decision":"BUY|SELL|HOLD","confidence":75,'
        '"bull":"bullish reason max 15 words",'
        '"bear":"bearish risk max 15 words",'
        '"action":"actionable advice max 20 words"}'
    )
    resp    = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=200,
    )
    content = resp.choices[0].message.content.strip()
    content = re.sub(r"^```[a-z]*\n?|\n?```$", "", content).strip()
    return json.loads(content)


# Targets: (label, emoji, tv_exchange, tv_symbol, tv_tf, cdc_exchange)
AI_TARGETS = [
    ("AAPL", "🍎", "nasdaq",  "AAPL",    "1D", "nasdaq"),
    ("BTC",  "₿",  "binance", "BTCUSDT", "1h", "binance"),
    ("GOLD", "🥇", None,      "GC=F",    "1D", "yahoo"),
]

for lbl, emo, tv_ex, tv_sym, tv_tf, cdc_ex in AI_TARGETS:
    blk      = [f"{emo} <b>AI ANALYSIS — {lbl}</b>", ""]
    tv_sig   = "N/A"
    cdc_zone = "-"
    comb_s   = "NEUTRAL"
    price_str = ""

    try:
        if tv_ex:
            d         = analyze_coin(tv_sym, tv_ex, tv_tf)
            tv_sig    = d.get("market_sentiment", {}).get("buy_sell_signal", "NEUTRAL")
            price     = d.get("price_data", {}).get("current_price", 0)
            price_str = f"${price:,.2f}" if price else ""
        cdc      = analyze_cdc(tv_sym, cdc_ex, tv_tf, tv_sig if tv_ex else None)
        cdc_zone = cdc.get("cdc_zone", "-")
        comb_s   = cdc.get("signal", "NEUTRAL")
        blk.append(f"📊 TV Signal: {tv_sig}")
        blk.append(f"{cdc.get('cdc_emoji','⚪')} CDC Zone: {cdc_zone}")
        blk.append(f"{cdc.get('sig_emoji','➖')} Combined: {comb_s}")
    except Exception as ex:
        blk.append(f"⚠️ Signal error: {ex}")

    blk.append("")

    try:
        ai  = deepseek_analyze(lbl, tv_sig, cdc_zone, comb_s, price_str)
        dec = str(ai.get("decision", "HOLD")).upper()
        pct = int(ai.get("confidence", 60))
        de  = {"BUY": "✅", "SELL": "❌", "HOLD": "⏸"}.get(dec, "❓")
        blk.append(f"🤖 AI Consensus: {de} <b>{dec}</b> ({pct}%)")
        if ai.get("bull"):   blk.append(f"🟢 Bull: {ai['bull']}")
        if ai.get("bear"):   blk.append(f"🔴 Bear: {ai['bear']}")
        if ai.get("action"): blk.append(f"📝 {ai['action']}")
    except Exception as ex:
        blk.append(f"🤖 AI: ⚠️ {str(ex)[:80]}")

    send("\n".join(blk))
    print(f"Step 5 {lbl} done")

print("All steps complete!")
