"""
CDC Action Zone V3 — Signal Engine
Calculates EMA12/EMA26 from OHLCV and classifies into 6 zones.
Combines with TradingView signal for high-confidence BUY/SELL.
"""
from __future__ import annotations
import urllib.request
import json
from typing import Optional


# ── EMA Calculator ─────────────────────────────────────────────────────────────

def calculate_ema(prices: list[float], period: int) -> list[float]:
    """Calculate EMA for a list of closing prices."""
    if len(prices) < period:
        return [prices[-1]] * len(prices)
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]  # SMA as seed
    for price in prices[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    # pad front so length matches prices
    pad = len(prices) - len(ema)
    return [ema[0]] * pad + ema


# ── CDC Zone Classifier ────────────────────────────────────────────────────────

def get_cdc_zone(close: float, ema12: float, ema26: float) -> dict:
    """
    Classify CDC Action Zone V3 based on position of Close, EMA12, EMA26.

    Returns dict with zone name, emoji, and raw bias (BUY/SELL/NEUTRAL).
    """
    if close > ema12 and ema12 > ema26:
        return {"zone": "Strong Bull", "emoji": "🔵", "bias": "BUY"}
    elif ema12 >= close >= ema26:
        return {"zone": "Weak Bull",   "emoji": "🟢", "bias": "BUY"}
    elif ema12 > ema26 and ema26 > close:
        return {"zone": "Trans. Up",   "emoji": "🟡", "bias": "NEUTRAL"}
    elif ema26 > ema12 and ema12 > close:
        return {"zone": "Trans. Down", "emoji": "🟠", "bias": "NEUTRAL"}
    elif ema26 >= close >= ema12:
        return {"zone": "Weak Bear",   "emoji": "🟤", "bias": "SELL"}
    else:
        return {"zone": "Strong Bear", "emoji": "🔴", "bias": "SELL"}


# ── Combined Signal ────────────────────────────────────────────────────────────

def combine_signals(tv_signal: str, cdc_bias: str) -> dict:
    """
    Combine TradingView summary signal with CDC zone bias.

    Both must agree → CONFIRMED. Conflict → CONFLICT. Both neutral → NEUTRAL.
    """
    tv_buy  = tv_signal in ("BUY", "STRONG_BUY")
    tv_sell = tv_signal in ("SELL", "STRONG_SELL")
    cdc_buy  = cdc_bias == "BUY"
    cdc_sell = cdc_bias == "SELL"

    if tv_buy and cdc_buy:
        return {"signal": "CONFIRMED BUY",  "emoji": "✅", "confidence": "HIGH"}
    elif tv_sell and cdc_sell:
        return {"signal": "CONFIRMED SELL", "emoji": "❌", "confidence": "HIGH"}
    elif (tv_buy and cdc_sell) or (tv_sell and cdc_buy):
        return {"signal": "CONFLICT",       "emoji": "⚠️", "confidence": "LOW"}
    else:
        return {"signal": "NEUTRAL",        "emoji": "➖", "confidence": "MEDIUM"}


# ── OHLCV Fetchers ─────────────────────────────────────────────────────────────

def fetch_ohlcv_crypto(symbol: str, interval: str = "1d", limit: int = 50) -> list[float]:
    """Fetch closing prices from Binance public API."""
    interval_map = {"1h": "1h", "4h": "4h", "1D": "1d", "1d": "1d"}
    iv = interval_map.get(interval, "1d")
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={iv}&limit={limit}"
    try:
        r = urllib.request.urlopen(url, timeout=10)
        data = json.loads(r.read())
        return [float(candle[4]) for candle in data]  # index 4 = close
    except Exception:
        return []


def fetch_ohlcv_yahoo(ticker: str, period: str = "3mo", interval: str = "1d") -> list[float]:
    """Fetch closing prices from Yahoo Finance via yfinance."""
    try:
        import yfinance as yf
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        if df.empty:
            return []
        # yfinance >= 0.2 returns MultiIndex columns e.g. ('Close', 'AAPL')
        if isinstance(df.columns, __import__('pandas').MultiIndex):
            close_col = [c for c in df.columns if c[0] == "Close"]
            if not close_col:
                return []
            return df[close_col[0]].dropna().tolist()
        return df["Close"].dropna().tolist()
    except Exception:
        return []


# ── Main Entry Point ───────────────────────────────────────────────────────────

def analyze_cdc(
    symbol: str,
    exchange: str,
    timeframe: str = "1D",
    tv_signal: Optional[str] = None,
) -> dict:
    """
    Fetch OHLCV, calculate CDC zone, combine with TV signal.

    Args:
        symbol:    e.g. BTCUSDT, AAPL, SCB
        exchange:  binance | nasdaq | nyse | set
        timeframe: 1h | 4h | 1D
        tv_signal: optional TradingView signal (BUY/SELL/NEUTRAL etc.)

    Returns dict with cdc zone + combined signal.
    """
    exchange = exchange.lower()
    closes: list[float] = []

    # --- fetch OHLCV ---
    if exchange in ("binance", "bybit", "kucoin", "mexc"):
        closes = fetch_ohlcv_crypto(symbol.upper(), interval=timeframe)
    elif exchange in ("nasdaq", "nyse"):
        closes = fetch_ohlcv_yahoo(symbol.upper(), period="3mo", interval="1d")
    elif exchange == "set":
        closes = fetch_ohlcv_yahoo(f"{symbol.upper()}.BK", period="3mo", interval="1d")
    else:
        # fallback yahoo
        closes = fetch_ohlcv_yahoo(symbol.upper(), period="3mo", interval="1d")

    if len(closes) < 26:
        return {"error": f"Not enough data ({len(closes)} candles)"}

    # --- calculate EMA ---
    ema12_series = calculate_ema(closes, 12)
    ema26_series = calculate_ema(closes, 26)

    close  = closes[-1]
    ema12  = ema12_series[-1]
    ema26  = ema26_series[-1]

    cdc = get_cdc_zone(close, ema12, ema26)

    result = {
        "close":  round(close,  4),
        "ema12":  round(ema12,  4),
        "ema26":  round(ema26,  4),
        "cdc_zone":  cdc["zone"],
        "cdc_emoji": cdc["emoji"],
        "cdc_bias":  cdc["bias"],
    }

    if tv_signal:
        combined = combine_signals(tv_signal, cdc["bias"])
        result.update({
            "tv_signal":  tv_signal,
            "signal":     combined["signal"],
            "sig_emoji":  combined["emoji"],
            "confidence": combined["confidence"],
        })

    return result
