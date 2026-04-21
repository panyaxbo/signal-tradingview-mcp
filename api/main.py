"""
TradingView MCP — FastAPI REST wrapper
Exposes the core service functions as HTTP endpoints.
"""
from __future__ import annotations

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tradingview_mcp.core.services.screener_service import (
    fetch_trending_analysis,
    analyze_coin,
)
from tradingview_mcp.core.services.scanner_service import (
    volume_breakout_scan,
    smart_volume_scan,
)
from tradingview_mcp.core.services.sentiment_service import analyze_sentiment
from tradingview_mcp.core.services.news_service import fetch_news_summary
from tradingview_mcp.core.services.yahoo_finance_service import get_price, get_market_snapshot
from tradingview_mcp.core.utils.validators import sanitize_exchange, sanitize_timeframe

app = FastAPI(
    title="TradingView MCP API",
    description="REST API wrapper for TradingView MCP — screener, scanner, sentiment, news",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


# ── Screener ──────────────────────────────────────────────────────────────────

@app.get("/api/top-gainers")
def top_gainers(
    exchange: str = Query("KUCOIN", description="Exchange: KUCOIN, BINANCE, BYBIT, MEXC, NASDAQ, NYSE"),
    timeframe: str = Query("15m", description="Timeframe: 5m, 15m, 1h, 4h, 1D"),
    limit: int = Query(25, ge=1, le=50),
):
    """Top gainers by Bollinger Band breakout."""
    exchange = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "15m")
    rows = fetch_trending_analysis(exchange, timeframe=timeframe, limit=limit)
    return [
        {"symbol": r["symbol"], "changePercent": r["changePercent"], "indicators": dict(r["indicators"])}
        for r in rows
    ]


@app.get("/api/top-losers")
def top_losers(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("15m"),
    limit: int = Query(25, ge=1, le=50),
):
    """Top losers."""
    exchange = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "15m")
    rows = fetch_trending_analysis(exchange, timeframe=timeframe, limit=limit, losers=True)
    return [
        {"symbol": r["symbol"], "changePercent": r["changePercent"], "indicators": dict(r["indicators"])}
        for r in rows
    ]


@app.get("/api/coin/{symbol}")
def coin_analysis(
    symbol: str,
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
):
    """Full technical analysis for a single coin."""
    exchange = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    try:
        result = analyze_coin(symbol.upper(), exchange, timeframe)
        return result
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


# ── Scanner ───────────────────────────────────────────────────────────────────

@app.get("/api/volume-breakout")
def volume_breakout(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
    limit: int = Query(20, ge=1, le=50),
):
    """Scan for volume breakout signals."""
    exchange = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    return volume_breakout_scan(exchange, timeframe=timeframe, limit=limit)


@app.get("/api/smart-volume")
def smart_volume(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
    limit: int = Query(20, ge=1, le=50),
):
    """Smart volume scanner — unusual accumulation detection."""
    exchange = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    return smart_volume_scan(exchange, timeframe=timeframe, limit=limit)


# ── Sentiment & News ──────────────────────────────────────────────────────────

@app.get("/api/sentiment")
def market_sentiment(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
):
    """Overall market sentiment analysis."""
    exchange = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    return analyze_sentiment(exchange, timeframe=timeframe)


@app.get("/api/news")
def financial_news(limit: int = Query(10, ge=1, le=50)):
    """Latest financial/crypto news summary."""
    return fetch_news_summary(limit=limit)


# ── Yahoo Finance ─────────────────────────────────────────────────────────────

@app.get("/api/price/{ticker}")
def yahoo_price(ticker: str):
    """Get price from Yahoo Finance (stocks, ETFs, crypto)."""
    try:
        return get_price(ticker.upper())
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/market-snapshot")
def market_snapshot():
    """Macro market snapshot — BTC, Gold, S&P500, DXY."""
    return get_market_snapshot()
