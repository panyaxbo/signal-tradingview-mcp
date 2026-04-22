"""
CDC Fresh Signal Scanner
-------------------------
Scans entire indices (Dow Jones 30, NASDAQ 100, S&P 500) for fresh CDC signals.

Candle rules:
  - Candle 1 (🆕): zone[-1] != zone[-2]           (just crossed into new zone)
  - Candle 2 (2️⃣): zone[-2] == zone[-1] != zone[-3] (confirmed 2nd candle)

Strategy:
  - Index symbols from DOW_30, NASDAQ_100 (hardcoded) + S&P 500 via screener
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
    "AAPL", "AMGN", "AXP", "BA",  "CAT", "CRM", "CSCO","CVX",
    "DIS",  "GS",   "HD",  "HON", "IBM", "INTC","JNJ", "JPM",
    "KO",   "MCD",  "MMM", "MRK", "MSFT","NKE", "PG",  "SHW",
    "TRV",  "UNH",  "V",   "VZ",  "WBA", "WMT",
]

NASDAQ_100 = [
    "AAPL","ABNB","ADBE","ADI","ADP","ADSK","AEP","AMAT","AMD","AMGN",
    "AMZN","ANSS","ARM","ASML","AVGO","AXON","AZN","BIIB","BKNG","BKR",
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
    "A","AAL","AAP","ABBV","ABT","ACGL","ACN","ADNT","AFL","AIG","AIZ",
    "AJG","AKAM","ALB","ALGN","ALK","ALL","ALLE","ANET","AON","AOS","APD",
    "APH","APTV","ARE","ATO","AVB","AVTR","AWK","AWR","AXP","AZO",
    "BAC","BAX","BDX","BEN","BG","BIO","BK","BKI","BLK","BMY","BR","BRK-B",
    "BRO","BSX","BWA","BX",
    "C","CAG","CB","CCI","CCL","CDAY","CF","CFG","CHD","CHRW","CI","CINF",
    "CL","CLX","CMA","CME","CMG","CMI","CMS","CNC","CNP","COF","COO",
    "COP","CPB","CPAY","CPT","CRL","CTRA","CTLT","CTVA","CVS","CVX",
    "D","DAL","DAY","DD","DE","DEO","DHI","DHR","DLR","DLTR","DOC","DOV",
    "DOW","DPZ","DRI","DTE","DUK","DVA","DVN","DXC","DXCM",
    "E","ECL","ED","EFX","EIX","EL","EMN","EMR","ENB","EOG","EPAM","EQR",
    "EQT","ES","ESS","ETN","ETR","ETSY","EVRG","EW","EXR",
    "F","FBHS","FDS","FDX","FE","FFIV","FI","FIS","FITB","FMC","FOX",
    "FOXA","FRT","FTV",
    "GD","GE","GEHC","GEN","GEV","GNRC","GPC","GPN","GPS","GS","GWW",
    "HAL","HAS","HBAN","HCA","HES","HIG","HII","HLT","HOLX","HPE","HPQ",
    "HRL","HSIC","HST","HSY","HUM","HWM",
    "ICE","IEX","IFF","INCY","IP","IPG","IQV","IR","IRM","IT","ITW","IVZ",
    "J","JBHT","JCI","JKHY","JNJ","JNPR","JPM",
    "K","KEY","KEYS","KIM","KMB","KMI","KMX","KR",
    "L","LDOS","LEN","LH","LHX","LIN","LKQ","LMT","LOW","LUV","LVS","LW",
    "LYB","LYV",
    "MA","MAA","MAS","MET","MGM","MHK","MKC","MLM","MO","MOH","MPW","MPC",
    "MPWR","MS","MSI","MTB","MTD","MTX",
    "NCLH","NEE","NEM","NFLX","NI","NOC","NOW","NRG","NSC","NTAP",
    "NTRS","NUE","NVR","NWS","NWSA",
    "O","OGN","OKE","OMC","ORCL","OXY",
    "PAYC","PFE","PFG","PH","PHM","PKG","PLD","PM","PNC","PNR","PNW","PODD",
    "POOL","PPG","PPL","PRU","PSA","PSX","PTC","PWR",
    "REG","RF","RHI","RJF","RL","RMD","ROK","ROL","RSG","RTX",
    "SBAC","SGEN","SHW","SJM","SLB","SNA","SOLV","SPG","SPGI","STE","STT",
    "STX","SWK","SWKS","SYF","SYK","SYY",
    "T","TAP","TDG","TDY","TEL","TER","TFC","TFX","TGT","TJX","TMO","TMUS",
    "TROW","TRV","TSCO","TSN","TT","TTWO",
    "UAL","UDR","UHS","ULTA","UPS","URI","USB",
    "VFC","VICI","VLO","VMC","VRSK","VTR",
    "WAB","WAT","WBA","WDC","WELL","WFC","WHR","WM","WRB","WRK","WST",
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
    Check if the most recent candle is the 1st or 2nd candle in a new CDC zone.

    Returns:
        (candle_num, zone_dict, price, ema12, ema26)  if fresh signal
        None                                           otherwise
    """
    if len(closes) < 30:
        return None

    e12 = calculate_ema(closes, 12)
    e26 = calculate_ema(closes, 26)

    z = [get_cdc_zone(closes[i], e12[i], e26[i]) for i in [-3, -2, -1]]
    cur = z[-1]

    if cur["bias"] == "NEUTRAL":
        return None

    # Candle 1: zone just changed
    if cur["zone"] != z[-2]["zone"]:
        return (1, cur, round(closes[-1], 4), round(e12[-1], 4), round(e26[-1], 4))

    # Candle 2: zone changed 1 candle ago, holding
    if z[-2]["zone"] == cur["zone"] and z[-3]["zone"] != cur["zone"]:
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
