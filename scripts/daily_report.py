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


# ── STEP 5 — AI Deep Analysis (TradingAgents + DeepSeek) ──────────────────────

# Install tradingagents if missing (supports both uv-managed and regular venvs)
try:
    import tradingagents  # noqa: F401
except ImportError:
    # Try uv pip first (uv-managed venvs don't have pip by default)
    result = subprocess.run(["uv", "pip", "install", "tradingagents"], capture_output=True)
    if result.returncode != 0:
        # Fallback: regular pip (for CCR / non-uv environments)
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "tradingagents"], check=True)


def _patch_ta_deepseek() -> None:
    """Patch TradingAgents in-place to add DeepSeek provider support."""
    base: str | None = None
    for d in site.getsitepackages():
        c = os.path.join(d, "tradingagents")
        if os.path.isdir(c):
            base = c
            break
    if not base:
        return

    changed = False

    # 1. default_config.py — add DEEPSEEK to LLMProvider enum
    p = os.path.join(base, "default_config.py")
    txt = open(p).read()
    if "DEEPSEEK" not in txt:
        for old, new in [
            ('XAI = "xai"', 'XAI = "xai"\n    DEEPSEEK = "deepseek"'),
            ("XAI = 'xai'", "XAI = 'xai'\n    DEEPSEEK = 'deepseek'"),
        ]:
            if old in txt:
                open(p, "w").write(txt.replace(old, new))
                changed = True
                break

    # 2. factory.py — add "deepseek" to OpenAI-compat list
    p = os.path.join(base, "llm_clients", "factory.py")
    txt = open(p).read()
    if '"deepseek"' not in txt and "'deepseek'" not in txt:
        for old, new in [
            ('"xai")', '"xai", "deepseek")'),
            ("'xai')", "'xai', 'deepseek')"),
        ]:
            if old in txt:
                open(p, "w").write(txt.replace(old, new))
                changed = True
                break

    # 3. openai_client.py — add DeepSeek base_url block
    p = os.path.join(base, "llm_clients", "openai_client.py")
    txt = open(p).read()
    if "deepseek" not in txt.lower():
        ds = (
            '        elif self.provider == "deepseek":\n'
            '            llm_kwargs["base_url"] = "https://api.deepseek.com/v1"\n'
            '            api_key = os.environ.get("DEEPSEEK_API_KEY")\n'
            '            if api_key:\n'
            '                llm_kwargs["api_key"] = api_key\n'
            "        "
        )
        for marker in ['        elif self.provider == "openai":', "        elif self.provider == 'openai':"]:
            if marker in txt:
                open(p, "w").write(txt.replace(marker, ds + marker.lstrip()))
                changed = True
                break

    if changed:
        for k in list(sys.modules.keys()):
            if "tradingagents" in k:
                del sys.modules[k]


_patch_ta_deepseek()

from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import TradingAgentsConfig, LLMProvider


def _clean(t: str) -> str:
    return re.sub(r"\*+|#+\s*", "", t).strip()


def _first_s(t: str, n: int = 140) -> str:
    if not t:
        return ""
    t = t.strip()
    for sep in [". ", "\n\n", "\n"]:
        i = t.find(sep)
        if 5 < i <= n:
            return t[: i + 1].strip()
    return t[:n].strip()


def _extract_pts(fd: str, bh: str, sh: str) -> tuple[str, str, str]:
    """Extract (bull_point, bear_point, headline) from state strings."""
    bull = bear = hl = ""
    if fd:
        ls = fd.split("\n")
        for line in ls[:6]:
            c = _clean(line)
            if c and any(k in c.upper() for k in ("HOLD", "BUY", "SELL")):
                hl = c[:200]
                break
        ib = is_ = False
        for line in ls:
            ll = line.lower()
            if any(k in ll for k in ("aggressive analyst", "buy/add", "bull")):
                ib, is_ = True, False
            elif any(k in ll for k in ("conservative analyst", "sell/trim", "bear", "risk analyst")):
                if ib:
                    ib, is_ = False, True
            s = line.strip()
            if s.startswith("- ") and len(s) > 12:
                pt = _clean(s[2:])[:140]
                if ib and not bull:
                    bull = pt
                elif is_ and not bear:
                    bear = pt
            if bull and bear:
                break

    def _from_h(h: str) -> str:
        if not h:
            return ""
        for pfx in ("Bull Analyst:", "Bear Analyst:", "Alright,"):
            i = h.find(pfx)
            if 0 <= i < 30:
                h = h[i + len(pfx) :].strip()
                break
        return _first_s(h, 130)

    if not bull:
        bull = _from_h(bh)
    if not bear:
        bear = _from_h(sh)
    return bull, bear, hl


def _conf_pct(dec: str, comb: str, conf: str) -> int:
    if dec == "HOLD":
        return 55
    ab, as_ = dec == "BUY", dec == "SELL"
    cb, cs = "BUY" in comb, "SELL" in comb
    if (ab and cb) or (as_ and cs):
        return {"HIGH": 82, "MEDIUM": 65, "LOW": 48}.get(conf, 60)
    if (ab and cs) or (as_ and cb):
        return 35
    return 52


def run_ta(ticker: str, comb_sig: str = "NEUTRAL", comb_conf: str = "MEDIUM") -> tuple:
    cfg = TradingAgentsConfig(
        llm_provider=LLMProvider.DEEPSEEK,
        deep_think_llm="deepseek-reasoner",
        quick_think_llm="deepseek-chat",
        max_debate_rounds=1,
        max_risk_discuss_rounds=1,
        max_recur_limit=100,
    )
    ta = TradingAgentsGraph(debug=False, config=cfg)
    state, decision = ta.propagate(ticker, today_date)
    dec = str(decision).strip().upper()
    inv = getattr(state, "investment_debate_state", None)
    bh  = getattr(inv, "bull_history", "") or ""
    sh  = getattr(inv, "bear_history", "") or ""
    fd  = getattr(state, "final_trade_decision", "") or ""
    bull, bear, hl = _extract_pts(fd, bh, sh)
    return dec, _conf_pct(dec, comb_sig, comb_conf), bull, bear, hl


# Targets: (ta_ticker, label, emoji, tv_exchange, tv_symbol, tv_tf, cdc_exchange)
AI_TARGETS = [
    ("AAPL",    "AAPL", "🍎", "nasdaq",  "AAPL",    "1D", "nasdaq"),
    ("BTC-USD", "BTC",  "₿",  "binance", "BTCUSDT", "1h", "binance"),
    ("GLD",     "GOLD", "🥇", None,      "GC=F",    "1D", "yahoo"),
]

for ta_tick, lbl, emo, tv_ex, tv_sym, tv_tf, cdc_ex in AI_TARGETS:
    blk = [f"{emo} <b>AI ANALYSIS — {lbl}</b>", ""]
    tv_sig = None
    comb_s = "NEUTRAL"
    comb_e = "➖"
    comb_c = "MEDIUM"

    try:
        if tv_ex:
            d      = analyze_coin(tv_sym, tv_ex, tv_tf)
            tv_sig = d.get("market_sentiment", {}).get("buy_sell_signal", "NEUTRAL")
            blk.append(f"📊 TV Signal: {tv_sig}")
        else:
            blk.append("📊 TV Signal: N/A")

        cdc    = analyze_cdc(tv_sym, cdc_ex, tv_tf, tv_sig)
        comb_s = cdc.get("signal", "NEUTRAL")
        comb_e = cdc.get("sig_emoji", "➖")
        comb_c = cdc.get("confidence", "MEDIUM")
        blk.append(f"{cdc.get('cdc_emoji', '⚪')} CDC Zone: {cdc.get('cdc_zone', '-')}")
        blk.append(f"{comb_e} Combined: {comb_s}")
    except Exception as ex:
        blk.append(f"⚠️ CDC error: {ex}")

    blk.append("")

    try:
        dec, pct, bull, bear, hl = run_ta(ta_tick, comb_s, comb_c)
        de = {"BUY": "✅", "SELL": "❌", "HOLD": "⏸"}.get(dec, "❓")
        blk.append(f"🤖 AI Consensus: {de} <b>{dec}</b> ({pct}%)")
        if bull:
            blk.append(f"🟢 Bull: {bull}")
        if bear:
            blk.append(f"🔴 Bear: {bear}")
        if hl:
            blk.append(f"📝 {hl}")
    except Exception as ex:
        blk.append(f"🤖 AI: ⚠️ {ex}")

    send("\n".join(blk))
    print(f"Step 5 {lbl} done")

print("All steps complete!")
