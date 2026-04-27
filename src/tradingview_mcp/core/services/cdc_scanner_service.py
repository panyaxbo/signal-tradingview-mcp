"""
CDC Fresh Signal Scanner + Wave 1→2 Bottoming Setup Scanner
------------------------------------------------------------
Scans entire indices (Dow Jones 30, NASDAQ 100, S&P 500).

CDC Fresh Signal rules:
  - Candle 1 (🆕): EMA12/EMA26 just crossed (fresh crossover)
  - Candle 2 (2️⃣): cross happened 1 candle ago, still holding

Wave 1→2 Bottoming Setup:
  - Prior downtrend ≥20% decline (EMA50 slope over ~6 months)
  - Swing Low detected (Wave 1 start / bottom)
  - Bounce ≥10% from bottom (Wave 1)
  - Retracement 30–90% of Wave 1 staying above bottom (Wave 2)
  - CDC status: fresh cross / just crossed / watch

Strategy:
  - OHLCV via yf.download() batch (one HTTP call for all symbols → fast)
  - Falls back to individual fetch_ohlcv_yahoo() if batch fails
  - CDC detection runs in parallel threads
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from tradingview_mcp.core.services.cdc_service import (
    calculate_ema,
    get_cdc_zone,
    fetch_ohlcv_yahoo,
)


# ── Index symbol lists ─────────────────────────────────────────────────────────

DOW_30 = [
    # WBA removed (Walgreens delisted from Dow, replaced by AMGEN/SHW)
    "AAPL", "AMGN", "AXP", "BA",  "CAT", "CRM", "CSCO","CVX",
    "DIS",  "GS",   "HD",  "HON", "IBM", "JNJ", "JPM",
    "KO",   "MCD",  "MMM", "MRK", "MSFT","NKE", "PG",  "SHW",
    "TRV",  "UNH",  "V",   "VZ",  "WMT",
]

NASDAQ_100 = [
    "AAPL","ABNB","ADBE","ADI","ADP","ADSK","AEP","AMAT","AMD","AMGN",
    "AMZN","ARM","ASML","AVGO","AXON","AZN","BIIB","BKNG","BKR",
    "CCEP","CDNS","CDW","CEG","CHTR","CMCSA","COST","CPRT","CRWD","CSCO",
    "CSGP","CSX","CTAS","CTSH","DASH","DDOG","DLTR","DXCM","EA","EXC",
    "FANG","FAST","FTNT","GEHC","GFS","GILD","GOOG","GOOGL","HON","IDXX",
    "ILMN","INTC","INTU","ISRG","KDP","KHC","KLAC","LIN","LRCX","LULU",
    "MAR","MCHP","MDB","MDLZ","MELI","META","MNST","MRNA","MRVL","MSFT",
    "MU",  "NFLX","NVDA","NXPI","ODFL","ON",  "ORLY","PANW","PAYX","PCAR",
    "PDD", "PEP", "PYPL","QCOM","REGN","ROST","ROP", "SBUX","SMCI","SNPS",
    "TEAM","TMUS","TSLA","TTD", "TTWO","TXN", "VRSK","VRTX","WBD", "WDAY",
    "XEL", "ZS",
]

SP_500_EXTRA = [
    # Large caps not already in DOW/NASDAQ lists
    # Removed delisted/acquired: BKI,CDAY,CMA,CTLT,DAY,FBHS,FI,GPS,HES,
    #   IPG,JNPR,K,MPW,SGEN,WBA,WRK,ANSS
    "A","AAL","AAP","ABBV","ABT","ACGL","ACN","AFL","AIG","AIZ",
    "AJG","AKAM","ALB","ALGN","ALK","ALL","ALLE","ANET","AON","AOS","APD",
    "APH","APTV","ARE","ATO","AVB","AVTR","AWK","AXP","AZO",
    "BAC","BAX","BDX","BEN","BG","BIO","BK","BLK","BMY","BR","BRK-B",
    "BRO","BSX","BWA","BX",
    "C","CAG","CB","CCI","CCL","CF","CFG","CHD","CHRW","CI","CINF",
    "CL","CLX","CME","CMG","CMI","CMS","CNC","CNP","COF","COO",
    "COP","CPB","CPAY","CPT","CRL","CTRA","CTVA","CVS","CVX",
    "D","DAL","DD","DE","DHI","DHR","DLR","DLTR","DOC","DOV",
    "DOW","DPZ","DRI","DTE","DUK","DVA","DVN","DXC","DXCM",
    "E","ECL","ED","EFX","EIX","EL","EMN","EMR","ENB","EOG","EPAM","EQR",
    "EQT","ES","ESS","ETN","ETR","ETSY","EVRG","EW","EXR",
    "F","FDS","FDX","FE","FFIV","FIS","FITB","FMC","FOX",
    "FOXA","FRT","FTV",
    "GD","GE","GEHC","GEN","GEV","GNRC","GPC","GPN","GS","GWW",
    "HAL","HAS","HBAN","HCA","HIG","HII","HLT","HOLX","HPE","HPQ",
    "HRL","HSIC","HST","HSY","HUM","HWM",
    "ICE","IEX","IFF","INCY","IP","IQV","IR","IRM","IT","ITW","IVZ",
    "J","JBHT","JCI","JKHY","JNJ","JPM",
    "KEY","KEYS","KIM","KMB","KMI","KMX","KR",
    "L","LDOS","LEN","LH","LHX","LIN","LKQ","LMT","LOW","LUV","LVS","LW",
    "LYB","LYV",
    "MA","MAA","MAS","MET","MGM","MHK","MKC","MLM","MO","MOH","MPC",
    "MPWR","MS","MSI","MTB","MTD","MTX",
    "NCLH","NEE","NEM","NI","NOC","NOW","NRG","NSC","NTAP",
    "NTRS","NUE","NVR","NWS","NWSA",
    "O","OGN","OKE","OMC","ORCL","OXY",
    "PAYC","PFE","PFG","PH","PHM","PKG","PLD","PM","PNC","PNR","PNW","PODD",
    "POOL","PPG","PPL","PRU","PSA","PSX","PTC","PWR",
    "REG","RF","RHI","RJF","RL","RMD","ROK","ROL","RSG","RTX",
    "SBAC","SHW","SJM","SLB","SNA","SOLV","SPG","SPGI","STE","STT",
    "STX","SWK","SWKS","SYF","SYK","SYY",
    "T","TAP","TDG","TDY","TEL","TER","TFC","TFX","TGT","TJX","TMO","TMUS",
    "TROW","TRV","TSCO","TSN","TT","TTWO",
    "UAL","UDR","UHS","ULTA","UPS","URI","USB",
    "VFC","VICI","VLO","VMC","VRSK","VTR",
    "WAB","WAT","WDC","WELL","WFC","WHR","WM","WRB","WST",
    "WTW","WY","WYNN",
    "XOM","XYL",
    "YUM","ZBH","ZBRA","ZTS",
]


def get_all_index_symbols() -> list[str]:
    """
    Return deduplicated union of DOW 30 + NASDAQ 100 + S&P 500 extra symbols.
    Tries to enrich with live screener data if available.
    """
    base = set(DOW_30) | set(NASDAQ_100) | set(SP_500_EXTRA)

    # Optional: try tradingview_screener for any large-cap US stocks we missed
    try:
        from tradingview_screener import Query, Column
        _, df = (
            Query()
            .select("name")
            .where(
                Column("exchange").isin(["NASDAQ", "NYSE"]),
                Column("market_cap_basic").gt(5_000_000_000),   # $5B+ market cap
                Column("type").eq("stock"),
                Column("is_primary").eq(True),
            )
            .order_by("market_cap_basic", ascending=False)
            .limit(600)
            .get_scanner_data()
        )
        screener_syms = set(df["name"].str.replace(r".*:", "", regex=True).tolist())
        base |= screener_syms
    except Exception:
        pass  # Screener unavailable — use hardcoded lists

    return sorted(base)


# ── Core detection ─────────────────────────────────────────────────────────────

def detect_fresh(closes: list[float]) -> Optional[tuple]:
    """
    Check if the most recent candle is the 1st or 2nd candle after a CDC bias flip.

    Detects EMA12/EMA26 crossover only — the true CDC Action Zone signal.

    Returns:
        (candle_num, zone_dict, price, ema12, ema26)  if fresh signal
        None                                           otherwise
    """
    if len(closes) < 30:
        return None

    e12 = calculate_ema(closes, 12)
    e26 = calculate_ema(closes, 26)

    cur = get_cdc_zone(closes[-1], e12[-1], e26[-1])

    # Core CDC signal: EMA12 vs EMA26 crossover (last 3 candles)
    ema_bull = [e12[i] > e26[i] for i in [-3, -2, -1]]

    # Candle 1: EMA12 just crossed EMA26 (fresh crossover this candle)
    if ema_bull[-1] != ema_bull[-2]:
        return (1, cur, round(closes[-1], 4), round(e12[-1], 4), round(e26[-1], 4))

    # Candle 2: crossover happened 1 candle ago, still holding direction
    if ema_bull[-1] == ema_bull[-2] and ema_bull[-2] != ema_bull[-3]:
        return (2, cur, round(closes[-1], 4), round(e12[-1], 4), round(e26[-1], 4))

    return None


# ── Batch OHLCV via yfinance ───────────────────────────────────────────────────

def _batch_fetch_closes(symbols: list[str], period: str = "3mo") -> dict[str, list[float]]:
    """
    Download close prices for all symbols in ONE yfinance batch call.

    Returns dict: {symbol: [close, ...]} (oldest → newest)
    """
    import yfinance as yf
    import pandas as pd

    # yfinance batch download
    data = yf.download(
        tickers=symbols,
        period=period,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )

    result: dict[str, list[float]] = {}

    if data.empty:
        return result

    if isinstance(data.columns, pd.MultiIndex):
        lvl0 = data.columns.get_level_values(0).unique().tolist()
        lvl1 = data.columns.get_level_values(1).unique().tolist()

        # yfinance ≥1.x: level 0 = ticker, level 1 = OHLCV  → data[sym]["Close"]
        # yfinance  old : level 0 = OHLCV,  level 1 = ticker → data["Close"][sym]
        ticker_first = "Close" not in lvl0 and any(s in lvl0 for s in symbols[:3])

        for sym in symbols:
            try:
                if ticker_first:
                    closes = data[sym]["Close"].dropna().tolist()
                else:
                    closes = data["Close"][sym].dropna().tolist()
                if len(closes) >= 30:
                    result[sym] = closes
            except Exception:
                pass
    else:
        # Single ticker — flat columns
        col = "Close" if "Close" in data.columns else "close"
        if col in data.columns and len(symbols) == 1:
            closes = data[col].dropna().tolist()
            if len(closes) >= 30:
                result[symbols[0]] = closes

    return result


# ── Scanner functions ──────────────────────────────────────────────────────────

def scan_yahoo(
    tickers: list[tuple[str, str, str]],   # [(ticker, label, emoji), ...]
    period: str = "3mo",
    interval: str = "1d",
) -> list[dict]:
    """
    Scan Yahoo Finance tickers for fresh CDC signals (sequential, small lists).

    Returns list of dicts: {ticker, label, emoji, candle, zone, price, ema12, ema26}
    """
    results = []
    for ticker, label, emoji in tickers:
        try:
            closes = fetch_ohlcv_yahoo(ticker, period=period, interval=interval)
            r = detect_fresh(closes)
            if r:
                candle, zone, price, e12, e26 = r
                results.append(dict(
                    ticker=ticker, label=label, emoji=emoji,
                    candle=candle, zone=zone, price=price, ema12=e12, ema26=e26,
                ))
        except Exception:
            pass
    return results


def scan_index_stocks(
    symbols: list[str] | None = None,
    max_results: int = 30,
) -> list[dict]:
    """
    Scan full US index universe (DOW 30 + NASDAQ 100 + S&P 500) for fresh CDC signals.

    Uses yf.download() batch for speed — one HTTP call for all symbols.
    Falls back to parallel individual fetch if batch fails.

    Args:
        symbols:     Custom symbol list. If None, uses get_all_index_symbols().
        max_results: Cap results (BUY and SELL counted separately upstream).

    Returns:
        List of result dicts sorted by (candle_num ASC, symbol ASC).
    """
    if symbols is None:
        symbols = get_all_index_symbols()

    # ── Batch download ─────────────────────────────────────────────────────────
    closes_map: dict[str, list[float]] = {}
    try:
        closes_map = _batch_fetch_closes(symbols)
    except Exception:
        pass

    # Fallback: parallel individual fetch for any symbol not in batch result
    missing = [s for s in symbols if s not in closes_map]
    if missing:
        def _fetch_one(sym):
            try:
                c = fetch_ohlcv_yahoo(sym, period="3mo", interval="1d")
                if len(c) >= 30:
                    return sym, c
            except Exception:
                pass
            return sym, None

        with ThreadPoolExecutor(max_workers=20) as ex:
            for sym, closes in ex.map(_fetch_one, missing):
                if closes:
                    closes_map[sym] = closes

    # ── CDC detection ──────────────────────────────────────────────────────────
    results: list[dict] = []
    for sym, closes in closes_map.items():
        r = detect_fresh(closes)
        if r:
            candle, zone, price, e12, e26 = r
            results.append(dict(
                ticker=sym, label=sym,
                emoji="📈" if zone["bias"] == "BUY" else "📉",
                candle=candle, zone=zone, price=price, ema12=e12, ema26=e26,
            ))

    results.sort(key=lambda x: (x["candle"], x["label"]))
    return results


# ── Legacy: parallel individual fetch (kept for backward compat) ───────────────

def scan_stocks_parallel(symbols: list[str], max_workers: int = 10) -> list[dict]:
    """Scan via parallel individual yfinance calls (slower than scan_index_stocks)."""
    def _check(sym):
        try:
            closes = fetch_ohlcv_yahoo(sym, period="3mo", interval="1d")
            r = detect_fresh(closes)
            if r:
                candle, zone, price, e12, e26 = r
                return dict(
                    ticker=sym, label=sym,
                    emoji="📈" if zone["bias"] == "BUY" else "📉",
                    candle=candle, zone=zone, price=price, ema12=e12, ema26=e26,
                )
        except Exception:
            pass
        return None

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for r in ex.map(_check, symbols):
            if r:
                results.append(r)
    results.sort(key=lambda x: (x["candle"], x["label"]))
    return results


# ── Telegram formatter ─────────────────────────────────────────────────────────

CANDLE_TAG = {1: "🆕", 2: "2️⃣"}


def format_fresh_section(
    title: str,
    results: list[dict],
    currency_symbol: str = "$",
    no_signal_text: str = "ไม่มี fresh signal วันนี้",
) -> str:
    """Build a Telegram HTML block for a group of fresh CDC results."""
    lines = [f"<b>{title}</b>", ""]
    if not results:
        lines.append(no_signal_text)
    else:
        for r in results:
            tag   = CANDLE_TAG.get(r["candle"], "")
            price = r["price"]
            zone  = r["zone"]
            lines.append(
                f"{zone['emoji']} {tag} <b>{r['label']}</b>  "
                f"{currency_symbol}{price:,.2f}  [{zone['zone']}]"
            )
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# Wave 1→2 Bottoming Setup Scanner
# ══════════════════════════════════════════════════════════════════════════════

# ── Swing Point Helpers ────────────────────────────────────────────────────────

def _find_swing_lows(prices: list[float], window: int = 8) -> list[tuple[int, float]]:
    """Local minimums: (index, price). Price[i] must be lowest in [i-w .. i+w]."""
    result = []
    n = len(prices)
    for i in range(window, n - window):
        lo = prices[i]
        if lo == min(prices[i - window: i + window + 1]):
            result.append((i, lo))
    return result


def _find_swing_highs(prices: list[float], window: int = 8) -> list[tuple[int, float]]:
    """Local maximums: (index, price). Price[i] must be highest in [i-w .. i+w]."""
    result = []
    n = len(prices)
    for i in range(window, n - window):
        hi = prices[i]
        if hi == max(prices[i - window: i + window + 1]):
            result.append((i, hi))
    return result


# ── Core Detection ─────────────────────────────────────────────────────────────

def detect_wave12_setup(
    closes: list[float],
    min_wave1_pct: float = 0.10,        # Wave 1 bounce must be ≥10%
    min_downtrend_pct: float = 0.20,    # Prior EMA50 drop must be ≥20%
    downtrend_lookback: int = 120,      # ~6 months of 1D candles for prior trend
    bottom_lookback: int = 90,          # Search for the absolute bottom in last 90 days
    min_days_since_bottom: int = 5,     # Bottom must be at least 5 days old
) -> Optional[dict]:
    """
    Detect Wave 1→2 bottoming setup after a prolonged downtrend.

    Algorithm:
      1. Prior downtrend  — EMA50 declined ≥20% over ~6 months
      2. Absolute bottom  — Lowest close in last `bottom_lookback` candles
      3. Wave 1           — Highest close AFTER the bottom (≥10% gain)
      4. Wave 2           — Current price retraced 25–90% of Wave 1, above bottom
      5. CDC status       — fresh_cross / just_crossed / bullish / watch

    Wave 1 peak = MAX close after bottom (no confirmed swing high required),
    so early-stage setups are caught before the peak is fully confirmed.

    Returns setup dict, or None if pattern not found.
    """
    min_len = downtrend_lookback + bottom_lookback + 10
    if len(closes) < min_len:
        return None

    e12 = calculate_ema(closes, 12)
    e26 = calculate_ema(closes, 26)
    e50 = calculate_ema(closes, 50)

    # ── 1. Prior Downtrend ─────────────────────────────────────────────────────
    e50_trend_start = e50[-(downtrend_lookback + bottom_lookback)]
    e50_at_bottom   = min(e50[-bottom_lookback:])
    if e50_trend_start <= 0:
        return None
    downtrend_pct = (e50_trend_start - e50_at_bottom) / e50_trend_start
    if downtrend_pct < min_downtrend_pct:
        return None

    # ── 2. Absolute Bottom (Wave 1 Start) ─────────────────────────────────────
    search        = closes[-bottom_lookback:]
    bot_local_idx = search.index(min(search))
    w1_start      = search[bot_local_idx]

    days_since_bot = len(search) - 1 - bot_local_idx
    if days_since_bot < min_days_since_bottom:
        return None     # Bottom too recent — not enough time for Wave 1 to form

    # ── 3. Wave 1 Peak — highest close after the bottom ───────────────────────
    post_bottom = search[bot_local_idx + 1:]
    if not post_bottom:
        return None

    w1_peak       = max(post_bottom)
    days_since_peak = len(post_bottom) - 1 - post_bottom.index(w1_peak)

    wave1_gain = (w1_peak - w1_start) / w1_start
    if wave1_gain < min_wave1_pct:
        return None     # Wave 1 too small — not a real bounce

    # ── 4. Wave 2 Retracement ──────────────────────────────────────────────────
    cur = closes[-1]
    if cur <= w1_start:
        return None     # Broke below bottom → Elliott Wave 2 rule violated

    wave1_range = w1_peak - w1_start
    retrace_pct = (w1_peak - cur) / wave1_range if wave1_range > 0 else 0

    if not (0.25 <= retrace_pct <= 0.90):
        return None     # Not in Wave 2 zone (25–90% retrace of Wave 1)

    fib_382 = w1_peak - 0.382 * wave1_range
    fib_500 = w1_peak - 0.500 * wave1_range
    fib_618 = w1_peak - 0.618 * wave1_range
    fib_786 = w1_peak - 0.786 * wave1_range

    fib_label = (
        "~38.2%" if retrace_pct < 0.45 else
        "~50.0%" if retrace_pct < 0.56 else
        "~61.8%" if retrace_pct < 0.71 else
        "~78.6%"
    )

    # ── 5. CDC Status ──────────────────────────────────────────────────────────
    ema_bull = [e12[i] > e26[i] for i in [-3, -2, -1]]
    if   ema_bull[-1] and not ema_bull[-2]:
        cdc_status = "fresh_cross"
    elif ema_bull[-1] and not ema_bull[-3]:
        cdc_status = "just_crossed"
    elif ema_bull[-1]:
        cdc_status = "bullish"
    else:
        cdc_status = "watch"    # Pattern ready — waiting for EMA12/EMA26 cross

    cdc_zone = get_cdc_zone(cur, e12[-1], e26[-1])

    return {
        "downtrend_pct":      round(downtrend_pct * 100, 1),
        "w1_start":           round(w1_start, 4),
        "w1_peak":            round(w1_peak,  4),
        "wave1_gain_pct":     round(wave1_gain * 100, 1),
        "retrace_pct":        round(retrace_pct * 100, 1),
        "fib_label":          fib_label,
        "fib_382":            round(fib_382, 4),
        "fib_500":            round(fib_500, 4),
        "fib_618":            round(fib_618, 4),
        "fib_786":            round(fib_786, 4),
        "current_price":      round(cur, 4),
        "cdc_status":         cdc_status,
        "cdc_zone":           cdc_zone,
        "days_since_bottom":  days_since_bot,
        "days_since_w1_peak": days_since_peak,
    }


# ── Scanner ────────────────────────────────────────────────────────────────────

def scan_wave12_setups(
    symbols: list[str] | None = None,
    period: str = "1y",
) -> list[dict]:
    """
    Scan US index stocks for Wave 1→2 bottoming setups.
    Uses 1-year data (batch download) for proper downtrend assessment.

    Returns list of result dicts sorted by:
      CDC priority (fresh_cross first) → retrace depth (deeper = more complete setup)
    """
    if symbols is None:
        symbols = get_all_index_symbols()

    # ── Batch download (1 year) ────────────────────────────────────────────────
    closes_map: dict[str, list[float]] = {}
    try:
        closes_map = _batch_fetch_closes(symbols, period=period)
    except Exception:
        pass

    # Fallback individual fetch
    missing = [s for s in symbols if s not in closes_map]
    if missing:
        def _fetch_one(sym):
            try:
                c = fetch_ohlcv_yahoo(sym, period=period, interval="1d")
                if len(c) >= 150:
                    return sym, c
            except Exception:
                pass
            return sym, None

        with ThreadPoolExecutor(max_workers=20) as ex:
            for sym, closes in ex.map(_fetch_one, missing):
                if closes:
                    closes_map[sym] = closes

    # ── Detect pattern ─────────────────────────────────────────────────────────
    results: list[dict] = []
    for sym, closes in closes_map.items():
        r = detect_wave12_setup(closes)
        if r:
            r["ticker"] = sym
            r["label"]  = sym
            results.append(r)

    # Sort: CDC status priority first, then deeper Wave 2 retracement first
    _pri = {"fresh_cross": 0, "just_crossed": 1, "watch": 2, "bullish": 3}
    results.sort(key=lambda x: (_pri.get(x["cdc_status"], 9), -x["retrace_pct"]))
    return results


# ── Telegram Formatter ─────────────────────────────────────────────────────────

_W12_CDC_LABEL = {
    "fresh_cross":  "🆕 CDC เพิ่ง cross bullish!",
    "just_crossed": "2️⃣ CDC cross เมื่อวาน (candle 2)",
    "bullish":      "✅ CDC bullish อยู่",
    "watch":        "⏳ รอ CDC cross (pattern ready)",
}


def format_wave12_section(
    title: str,
    results: list[dict],
    no_signal_text: str = "ไม่มี Wave 1→2 setup วันนี้",
) -> str:
    """Format Wave 1→2 setups as Telegram HTML."""
    lines = [f"<b>{title}</b>", ""]
    if not results:
        lines.append(no_signal_text)
    else:
        for r in results:
            zone = r["cdc_zone"]
            lines.append(
                f"{zone['emoji']} <b>{r['label']}</b>  ${r['current_price']:,.2f}\n"
                f"   📉 Downtrend: -{r['downtrend_pct']}%"
                f"  |  W1: +{r['wave1_gain_pct']}%\n"
                f"   📐 W2 retrace: {r['retrace_pct']}% {r['fib_label']}"
                f"  |  Bottom: ${r['w1_start']:,.2f}\n"
                f"   Fib 61.8%: ${r['fib_618']:,.2f}"
                f"  |  78.6%: ${r['fib_786']:,.2f}\n"
                f"   {_W12_CDC_LABEL.get(r['cdc_status'], r['cdc_status'])}"
            )
            lines.append("")
    return "\n".join(lines)
