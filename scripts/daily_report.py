"""
Daily Market Report — Telegram Bot
Runs every morning via APScheduler (FastAPI) or Claude Remote Trigger.

Sections:
  1. Starting message
  2. Top 20 Gainers (Binance 1h)
  3. Top 20 Losers  (Binance 1h)
  4. Watchlist — TV signal + CDC Action Zone
  5. AI Deep Analysis — DeepSeek (configurable targets)
  6. CDC Fresh Signal Scanner — DOW/NASDAQ/S&P (configurable)

Config: ../config.json
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.request
import warnings
warnings.filterwarnings("ignore")   # suppress tradingview_ta interval warnings

# ── Path setup ─────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# ── Load config ────────────────────────────────────────────────────────────────
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.json")

def _load_cfg() -> dict:
    try:
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

CFG = _load_cfg()

# ── Credentials ────────────────────────────────────────────────────────────────
BOT  = "8720452318:AAGgh2WXUW6JFw_Z71eMUBZ0bi-n5eHnwuE"
CHAT = "5636156156"
BASE = "https://signal-tradingview-mcp.up.railway.app"
os.environ.setdefault("DEEPSEEK_API_KEY", "sk-9d9423f9ab86437197bbe96180d401e3")

# ── Helpers ────────────────────────────────────────────────────────────────────

def send(msg: str) -> bool:
    for chunk in [msg[i:i+4000] for i in range(0, max(len(msg),1), 4000)]:
        payload = json.dumps({"chat_id": CHAT, "text": chunk, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{BOT}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            json.loads(urllib.request.urlopen(req).read())
        except Exception as e:
            print("Send error:", e)
    return True


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
send(f"🤖 <b>Daily Market Report</b>\n🗓 {today} | กำลังดึงข้อมูล รอสักครู่นะครับ...")
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

wl = CFG.get("watchlist", {})

lines = ["🎯 <b>WATCHLIST — TECHNICAL SIGNALS</b>", ""]

# Crypto
crypto_list = wl.get("crypto", [
    ["BTCUSDT","BTC"],["ETHUSDT","ETH"],["SOLUSDT","SOL"],["XRPUSDT","XRP"]
])
lines.append("─── CRYPTO ───")
for item in crypto_list:
    sym, label = (item[0], item[1]) if isinstance(item, list) else (item, item)
    try:
        d     = analyze_coin(sym, "binance", "1h")
        sig   = d.get("market_sentiment", {}).get("buy_sell_signal", "N/A")
        price = d.get("price_data", {}).get("current_price", 0)
        if not price:
            r2    = json.loads(urllib.request.urlopen(
                f"https://api.binance.com/api/v3/ticker/price?symbol={sym}", timeout=5).read())
            price = float(r2.get("price", 0))
        rsi   = d.get("rsi", {}).get("value", "N/A")
        rsi_s = f"{rsi:.1f}" if isinstance(rsi, float) else str(rsi)
        cdc   = analyze_cdc(sym, "binance", "1h", sig)
        lines.append(f"{sig_emoji(sig)} <b>{label}</b>  ${price:,.2f}  |  {sig}  |  RSI:{rsi_s}  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{label}</b>  N/A")

# Commodities
comm_list = wl.get("commodities", [
    ["GC=F","GOLD","🥇"],["SI=F","SILVER","🥈"],["HG=F","COPPER","🥉"],["CL=F","WTI","🛢"]
])
lines += ["", "─── COMMODITIES ───"]
for item in comm_list:
    ticker, label = item[0], item[1]
    emoji = item[2] if len(item) > 2 else "📊"
    try:
        d   = get_price(ticker)
        cdc = analyze_cdc(ticker, "yahoo", "1D")
        lines.append(f"{emoji} <b>{label}</b>  ${d['price']:,.2f}  ({d['change_pct']:+.2f}%)  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{label}</b>  N/A")

# Stocks US
us_list = wl.get("stocks_us", ["AAPL","MSFT","TSLA","NVDA","AMZN","META","GOOG","PRCT","TGLS","IREN","VT","JEPQ"])
lines += ["", "─── STOCKS US ───"]
for sym in us_list:
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
th_list = wl.get("stocks_th", ["SCB","KTB","BCPG"])
lines += ["", "─── STOCKS TH ───"]
for sym in th_list:
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


# ── STEP 5 — AI Deep Analysis ─────────────────────────────────────────────────

def deepseek_analyze(label: str, tv_sig: str, cdc_zone: str, combined: str, price_str: str) -> dict:
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


AI_TARGETS = CFG.get("ai_targets", [
    ["AAPL", "🍎", "nasdaq",  "AAPL",    "1D", "nasdaq"],
    ["BTC",  "₿",  "binance", "BTCUSDT", "1h", "binance"],
    ["GOLD", "🥇", None,      "GC=F",    "1D", "yahoo"],
])

for item in AI_TARGETS:
    lbl, emo = item[0], item[1]
    tv_ex    = item[2] if len(item) > 2 else None
    tv_sym   = item[3] if len(item) > 3 else lbl
    tv_tf    = item[4] if len(item) > 4 else "1D"
    cdc_ex   = item[5] if len(item) > 5 else "yahoo"

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


# ── STEP 6 — CDC Fresh Signal Scanner ────────────────────────────────────────
from tradingview_mcp.core.services.cdc_scanner_service import (
    scan_yahoo, scan_index_stocks, get_all_index_symbols, format_fresh_section,
    DOW_30, NASDAQ_100, SP_500_EXTRA,
)

cdc_cfg = CFG.get("cdc_scanner", {})

send("🔍 <b>CDC Fresh Signal Scanner (1D)</b>\n⏳ กำลัง scan รอสักครู่...")

# Commodities
if cdc_cfg.get("commodities", True):
    comm_tickers = [
        (item[0], item[1], item[2] if len(item) > 2 else "📊")
        for item in comm_list
    ]
    comm_fresh = scan_yahoo(comm_tickers)
    send(format_fresh_section("🏅 CDC FRESH SIGNAL — COMMODITIES (1D)", comm_fresh))

# Build stock universe based on config toggles
universe: list[str] = []
if cdc_cfg.get("dow30",    True):  universe += DOW_30
if cdc_cfg.get("nasdaq100",True):  universe += NASDAQ_100
if cdc_cfg.get("sp500",    True):  universe += SP_500_EXTRA

# Deduplicate
universe = sorted(set(universe))

if universe:
    us_fresh = scan_index_stocks(symbols=universe)
    us_buy   = [r for r in us_fresh if r["zone"]["bias"] == "BUY"]
    us_sell  = [r for r in us_fresh if r["zone"]["bias"] == "SELL"]

    indices_on = [k for k in ["dow30","nasdaq100","sp500"] if cdc_cfg.get(k, True)]
    lbl = "/".join({"dow30":"DOW","nasdaq100":"NASDAQ","sp500":"S&P"}.get(k,"") for k in indices_on)

    send(format_fresh_section(
        f"📈 CDC FRESH BUY — {lbl} (1D)  ({len(us_buy)} ตัว)",
        us_buy,
        no_signal_text="ไม่มี fresh BUY signal วันนี้",
    ))
    send(format_fresh_section(
        f"📉 CDC FRESH SELL — {lbl} (1D)  ({len(us_sell)} ตัว)",
        us_sell,
        no_signal_text="ไม่มี fresh SELL signal วันนี้",
    ))

print("Step 6 done")
print("All steps complete!")
