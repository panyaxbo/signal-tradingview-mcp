"""
CDC Fresh Signal Scanner
-------------------------
Scans a list of tickers and returns only those on the 1st or 2nd candle
of a new CDC Action Zone (EMA-12/EMA-26 crossover-based zones).

Candle rules:
  - Candle 1: zone[-1] != zone[-2]           (just crossed into new zone)
  - Candle 2: zone[-2] == zone[-1] != zone[-3] (one candle ago crossed, still in new zone)

Supports:
  - Yahoo Finance tickers (commodities, US/TH stocks)
  - Binance crypto (via tradingview_ta)
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from tradingview_mcp.core.services.cdc_service import (
    calculate_ema,
    get_cdc_zone,
    fetch_ohlcv_yahoo,
)


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

    if cur["zone"] != z[-2]["zone"]:
        return (1, cur, round(closes[-1], 4), round(e12[-1], 4), round(e26[-1], 4))

    if z[-2]["zone"] == cur["zone"] and z[-3]["zone"] != cur["zone"]:
        return (2, cur, round(closes[-1], 4), round(e12[-1], 4), round(e26[-1], 4))

    return None


# ── Scanners ───────────────────────────────────────────────────────────────────

def scan_yahoo(
    tickers: list[tuple[str, str, str]],   # [(ticker, label, emoji), ...]
    period: str = "3mo",
    interval: str = "1d",
) -> list[dict]:
    """
    Scan Yahoo Finance tickers for fresh CDC signals.

    Returns list of dicts: {label, emoji, candle, zone, price}
    """
    results = []
    for ticker, label, emoji in tickers:
        try:
            closes = fetch_ohlcv_yahoo(ticker, period=period, interval=interval)
            r = detect_fresh(closes)
            if r:
                candle, zone, price, e12, e26 = r
                results.append(
                    dict(
                        ticker=ticker,
                        label=label,
                        emoji=emoji,
                        candle=candle,
                        zone=zone,
                        price=price,
                        ema12=e12,
                        ema26=e26,
                    )
                )
        except Exception:
            pass
    return results


def _check_yahoo_single(sym: str) -> Optional[dict]:
    """Worker for parallel US stock scanning."""
    try:
        closes = fetch_ohlcv_yahoo(sym, period="3mo", interval="1d")
        r = detect_fresh(closes)
        if r:
            candle, zone, price, e12, e26 = r
            return dict(
                ticker=sym,
                label=sym,
                emoji="📈" if zone["bias"] == "BUY" else "📉",
                candle=candle,
                zone=zone,
                price=price,
                ema12=e12,
                ema26=e26,
            )
    except Exception:
        pass
    return None


def scan_stocks_parallel(
    symbols: list[str],
    max_workers: int = 10,
) -> list[dict]:
    """
    Scan a list of stock symbols in parallel for fresh CDC signals.

    Returns list of dicts sorted by candle number then symbol.
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_check_yahoo_single, s): s for s in symbols}
        for f in as_completed(futures):
            r = f.result()
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
            tag = CANDLE_TAG.get(r["candle"], "")
            price = r["price"]
            zone = r["zone"]
            price_fmt = (
                f"{currency_symbol}{price:,.2f}"
                if currency_symbol == "$"
                else f"{currency_symbol}{price:,.2f}"
            )
            lines.append(
                f"{zone['emoji']} {tag} <b>{r['label']}</b>  {price_fmt}  [{zone['zone']}]"
            )
    return "\n".join(lines)
