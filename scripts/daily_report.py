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
    # Split at blank-line boundaries to avoid cutting HTML tags mid-entry
    entries = msg.split("\n\n")
    chunks, cur = [], ""
    for e in entries:
        block = e + "\n\n"
        if len(cur) + len(block) > 3800:
            if cur.strip():
                chunks.append(cur.strip())
            cur = block
        else:
            cur += block
    if cur.strip():
        chunks.append(cur.strip())
    if not chunks:
        chunks = [msg]

    for chunk in chunks:
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


# ── STEP 2 — Watchlist ────────────────────────────────────────────────────────
# Use own Railway API endpoint → avoids TradingView rate-limit on Railway IP
from tradingview_mcp.core.services.cdc_service import analyze_cdc

wl = CFG.get("watchlist", {})

def coin_via_api(sym: str, exchange: str, timeframe: str) -> dict:
    """Fetch coin analysis via our own API (avoids direct TradingView rate-limit)."""
    url = f"{BASE}/api/coin/{sym}?exchange={exchange}&timeframe={timeframe}"
    d = fetch(url)
    return d or {}

lines = ["🎯 <b>WATCHLIST — TECHNICAL SIGNALS</b>", ""]

# Crypto
crypto_list = wl.get("crypto", [
    ["BTCUSDT","BTC"],["ETHUSDT","ETH"],["SOLUSDT","SOL"],["XRPUSDT","XRP"]
])
lines.append("─── CRYPTO ───")
for item in crypto_list:
    sym, label = (item[0], item[1]) if isinstance(item, list) else (item, item)
    try:
        d     = coin_via_api(sym, "binance", "1h")
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
        d   = fetch(f"{BASE}/api/price/{ticker}")
        cdc = analyze_cdc(ticker, "yahoo", "1D")
        price = d.get("price", 0) if d else 0
        chg   = d.get("change_pct", 0) if d else 0
        lines.append(f"{emoji} <b>{label}</b>  ${price:,.2f}  ({chg:+.2f}%)  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{label}</b>  N/A")

# Stocks US
us_list = wl.get("stocks_us", ["AAPL","MSFT","TSLA","NVDA","AMZN","META","GOOG","PRCT","TGLS","IREN","VT","JEPQ"])
lines += ["", "─── STOCKS US ───"]
for sym in us_list:
    try:
        d     = coin_via_api(sym, "nasdaq", "1D")
        sig   = d.get("market_sentiment", {}).get("buy_sell_signal", "N/A")
        price = d.get("price_data", {}).get("current_price", 0)
        if not price:
            pd = fetch(f"{BASE}/api/price/{sym}")
            price = pd.get("price", 0) if pd else 0
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
        d     = coin_via_api(sym, "set", "1D")
        sig   = d.get("market_sentiment", {}).get("buy_sell_signal", "N/A")
        price = d.get("price_data", {}).get("current_price", 0)
        if not price:
            pd = fetch(f"{BASE}/api/price/{sym}.BK")
            price = pd.get("price", 0) if pd else 0
        rsi   = d.get("rsi", {}).get("value", "N/A")
        rsi_s = f"{rsi:.1f}" if isinstance(rsi, float) else str(rsi)
        cdc   = analyze_cdc(sym, "set", "1D", sig)
        lines.append(f"{sig_emoji(sig)} <b>{sym}</b>  ฿{price:,.2f}  |  {sig}  |  RSI:{rsi_s}  |  {cdc['sig_emoji']}{cdc['signal']}")
    except Exception:
        lines.append(f"⚠️ <b>{sym}</b>  N/A")

send("\n".join(lines))
print("Step 2 done")


# ── STEP 3 — AI Deep Analysis ─────────────────────────────────────────────────

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
    print(f"Step 3 {lbl} done")


# ── STEP 4 — Full Elliott Wave Scanner (all 6 patterns, download once) ────────
from tradingview_mcp.core.services.cdc_scanner_service import (
    scan_all_setups,
    format_wave12_section, format_waveab_section,
    format_wave3_section,  format_wavec_section,
    format_wave45_section, format_wave45_bear_section,
    DOW_30, NASDAQ_100, SP_500_EXTRA,
)

cdc_cfg = CFG.get("cdc_scanner", {})

if cdc_cfg.get("wave12", True):
    send("📐 <b>Elliott Wave Scanner</b>\n⏳ กำลัง scan 6 patterns (ใช้ข้อมูล 1 ปี)...")

    scan_universe: list[str] = []
    if cdc_cfg.get("dow30",     True): scan_universe += DOW_30
    if cdc_cfg.get("nasdaq100", True): scan_universe += NASDAQ_100
    if cdc_cfg.get("sp500",     True): scan_universe += SP_500_EXTRA
    scan_universe = sorted(set(scan_universe))

    w12_results = wab_results = w3_results = wc_results = w45_results = w45b_results = []

    if scan_universe:
        # Download ONCE → detect all 6 patterns in a single pass
        w12_results, wab_results, w3_results, wc_results, w45_results, w45b_results = \
            scan_all_setups(symbols=scan_universe, period="1y")

        # ── Wave 1→2 (Bottoming setup, bullish) ──────────────────────────────
        w12_ready = [r for r in w12_results if r["cdc_status"] in ("fresh_cross", "just_crossed")]
        w12_watch = [r for r in w12_results if r["cdc_status"] in ("watch", "bullish")]
        send(format_wave12_section(
            f"🐂 WAVE 1→2 — CDC CONFIRMED ({len(w12_ready)} ตัว)",
            w12_ready,
            no_signal_text="ไม่มี Wave 1→2 ที่ CDC confirm วันนี้",
        ))
        send(format_wave12_section(
            f"🐂 WAVE 1→2 — WATCH LIST ({len(w12_watch)} ตัว)",
            w12_watch,
            no_signal_text="ไม่มี Wave 1→2 ที่กำลัง form วันนี้",
        ))

        # ── Wave A→B (Topping setup, bearish) ────────────────────────────────
        wab_ready = [r for r in wab_results if r["cdc_status"] in ("fresh_cross_down", "just_crossed_down")]
        wab_watch = [r for r in wab_results if r["cdc_status"] in ("watch_bear", "bearish")]
        send(format_waveab_section(
            f"🐻 WAVE A→B — CDC CONFIRMED ({len(wab_ready)} ตัว)",
            wab_ready,
            no_signal_text="ไม่มี Wave A→B ที่ CDC confirm วันนี้",
        ))
        send(format_waveab_section(
            f"🐻 WAVE A→B — WATCH LIST ({len(wab_watch)} ตัว)",
            wab_watch,
            no_signal_text="ไม่มี Wave A→B ที่กำลัง form วันนี้",
        ))

        # ── Wave 3 Breakout (bull) + Wave C Breakdown (bear) ─────────────────
        w3_cdc   = [r for r in w3_results  if r["cdc_status"] in ("fresh_cross", "just_crossed")]
        w3_bull  = [r for r in w3_results  if r["cdc_status"] in ("bullish", "watch")]
        wc_cdc   = [r for r in wc_results  if r["cdc_status"] in ("fresh_cross_down", "just_crossed_down")]
        wc_bear  = [r for r in wc_results  if r["cdc_status"] in ("bearish", "watch_bear")]
        send(format_wave3_section(
            f"🚀 WAVE 3 BREAKOUT — CDC ({len(w3_cdc)} ตัว)",
            w3_cdc,
            no_signal_text="ไม่มี Wave 3 breakout ที่ CDC confirm วันนี้",
        ))
        send(format_wave3_section(
            f"🚀 WAVE 3 BREAKOUT — IN PROGRESS ({len(w3_bull)} ตัว)",
            w3_bull,
            no_signal_text="ไม่มี Wave 3 ที่กำลัง run วันนี้",
        ))
        send(format_wavec_section(
            f"📉 WAVE C BREAKDOWN — CDC ({len(wc_cdc)} ตัว)",
            wc_cdc,
            no_signal_text="ไม่มี Wave C breakdown ที่ CDC confirm วันนี้",
        ))
        send(format_wavec_section(
            f"📉 WAVE C BREAKDOWN — IN PROGRESS ({len(wc_bear)} ตัว)",
            wc_bear,
            no_signal_text="ไม่มี Wave C ที่กำลัง run วันนี้",
        ))

        # ── Wave 4→5 (bull) + Wave 4→5 Bear ─────────────────────────────────
        w45_entry = [r for r in w45_results  if r["cdc_status"] in ("w5_starting", "w5_confirmed")]
        w45_wait  = [r for r in w45_results  if r["cdc_status"] in ("w4_fresh", "in_w4", "watch")]
        w45b_entry = [r for r in w45b_results if r["cdc_status"] in ("w5_starting", "w5_confirmed")]
        w45b_wait  = [r for r in w45b_results if r["cdc_status"] in ("w4_fresh", "in_w4_bounce", "watch_bear")]
        send(format_wave45_section(
            f"⚡ WAVE 4→5 BULL — W5 STARTING ({len(w45_entry)} ตัว)",
            w45_entry,
            no_signal_text="ไม่มี Wave 5 bull ที่กำลังเริ่มวันนี้",
        ))
        send(format_wave45_section(
            f"⚡ WAVE 4→5 BULL — IN W4 PULLBACK ({len(w45_wait)} ตัว)",
            w45_wait,
            no_signal_text="ไม่มี Wave 4 pullback setup วันนี้",
        ))
        send(format_wave45_bear_section(
            f"⚡ WAVE 4→5 BEAR — W5 STARTING ({len(w45b_entry)} ตัว)",
            w45b_entry,
            no_signal_text="ไม่มี Wave 5 bear ที่กำลังเริ่มวันนี้",
        ))
        send(format_wave45_bear_section(
            f"⚡ WAVE 4→5 BEAR — IN W4 BOUNCE ({len(w45b_wait)} ตัว)",
            w45b_wait,
            no_signal_text="ไม่มี Wave 4 bear bounce setup วันนี้",
        ))

    # ── Sync W1→2 + WA→B to watchlist API (tracks invalidation & confirmation) ─
    all_wave_results = w12_results + wab_results
    if all_wave_results:
        payload = json.dumps(all_wave_results, ensure_ascii=False).encode()
        req = urllib.request.Request(
            f"{BASE}/api/wave12-watchlist/sync",
            data=payload, headers={"Content-Type": "application/json"},
        )
        try:
            res = json.loads(urllib.request.urlopen(req, timeout=15).read())
            print(f"Step 4: watchlist synced — +{res.get('added',0)} new, {res.get('total_watching',0)} watching")
        except Exception as e:
            print(f"Step 4: watchlist sync warning: {e}")

print("Step 4 done")
print("All steps complete!")
