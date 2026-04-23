"""
TradingView MCP — FastAPI REST wrapper
Exposes the core service functions as HTTP endpoints.
Includes Web Admin UI, APScheduler for daily report, config management.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from collections import deque
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from typing import Any

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

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT      = Path(__file__).parent.parent
CONFIG_DEFAULT = REPO_ROOT / "config.json"       # git default (read-only on Railway)
CONFIG_RUNTIME = Path("/tmp/report_config.json") # persists within deployment
SCRIPT_PATH    = REPO_ROOT / "scripts" / "daily_report.py"
ADMIN_HTML     = Path(__file__).parent / "admin.html"

# ── Config helpers ─────────────────────────────────────────────────────────────

def load_config() -> dict:
    # Prefer runtime config (user edits) over git default
    for path in [CONFIG_RUNTIME, CONFIG_DEFAULT]:
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_config(cfg: dict) -> None:
    # Save to /tmp so it survives within this deployment
    with open(CONFIG_RUNTIME, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    # Also try to write back to repo root (works locally, read-only on Railway)
    try:
        with open(CONFIG_DEFAULT, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

# ── Run-log state ──────────────────────────────────────────────────────────────
_run_log: deque[str] = deque(maxlen=500)
_running  = False
_run_lock = threading.Lock()

def _do_run() -> None:
    global _running
    try:
        env = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")}
        proc = subprocess.Popen(
            [sys.executable, str(SCRIPT_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(REPO_ROOT),
            env=env,
        )
        for line in proc.stdout:
            _run_log.append(line.rstrip())
        proc.wait()
        _run_log.append(f"[exit code {proc.returncode}]")
    except Exception as exc:
        _run_log.append(f"[ERROR] {exc}")
    finally:
        _running = False

def trigger_report() -> None:
    global _running
    with _run_lock:
        if _running:
            return
        _running = True
    _run_log.clear()
    threading.Thread(target=_do_run, daemon=True).start()

# ── Scheduler ──────────────────────────────────────────────────────────────────
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler()

def _apply_schedule(cfg: dict) -> None:
    sch = cfg.get("schedule", {})
    hour   = int(sch.get("hour", 7))
    minute = int(sch.get("minute", 0))
    tz     = sch.get("timezone", "Asia/Bangkok")
    scheduler.add_job(
        trigger_report,
        CronTrigger(hour=hour, minute=minute, timezone=tz),
        id="daily_report",
        replace_existing=True,
    )

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="TradingView MCP API",
    description="REST API wrapper for TradingView MCP — screener, scanner, sentiment, news, admin",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup() -> None:
    cfg = load_config()
    _apply_schedule(cfg)
    scheduler.start()

@app.on_event("shutdown")
def shutdown() -> None:
    scheduler.shutdown(wait=False)

# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}

# ── Admin UI ───────────────────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
def admin_ui():
    return ADMIN_HTML.read_text(encoding="utf-8")

# ── Config endpoints ───────────────────────────────────────────────────────────

@app.get("/api/config")
def get_config():
    return load_config()

@app.post("/api/config")
def post_config(body: dict[str, Any] = Body(...)):
    save_config(body)
    _apply_schedule(body)
    sch = body.get("schedule", {})
    return {
        "ok": True,
        "schedule": f"{sch.get('hour',7):02d}:{sch.get('minute',0):02d} {sch.get('timezone','Asia/Bangkok')}",
    }

# ── Run / Log endpoints ────────────────────────────────────────────────────────

@app.post("/api/run")
def run_now():
    if _running:
        return {"ok": False, "message": "Already running"}
    trigger_report()
    return {"ok": True, "message": "Started"}

@app.get("/api/run/status")
def run_status():
    return {
        "running": _running,
        "log": list(_run_log),
    }

# ── Schedule info ──────────────────────────────────────────────────────────────

@app.get("/api/schedule")
def schedule_info():
    job = scheduler.get_job("daily_report")
    nxt = str(job.next_run_time) if job else None
    cfg = load_config().get("schedule", {})
    return {"next_run": nxt, "schedule": cfg}

# ── Screener ──────────────────────────────────────────────────────────────────

@app.get("/api/top-gainers")
def top_gainers(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("15m"),
    limit: int = Query(25, ge=1, le=50),
):
    exchange  = sanitize_exchange(exchange, "KUCOIN")
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
    exchange  = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "15m")
    rows = fetch_trending_analysis(exchange, timeframe=timeframe, limit=500)
    losers = sorted(rows, key=lambda r: r["changePercent"])[:limit]
    return [
        {"symbol": r["symbol"], "changePercent": r["changePercent"], "indicators": dict(r["indicators"])}
        for r in losers
    ]

@app.get("/api/coin/{symbol}")
def coin_analysis(
    symbol: str,
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
):
    exchange  = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    try:
        return analyze_coin(symbol.upper(), exchange, timeframe)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# ── Scanner ───────────────────────────────────────────────────────────────────

@app.get("/api/volume-breakout")
def volume_breakout(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
    limit: int = Query(20, ge=1, le=50),
):
    exchange  = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    return volume_breakout_scan(exchange, timeframe=timeframe, limit=limit)

@app.get("/api/smart-volume")
def smart_volume(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
    limit: int = Query(20, ge=1, le=50),
):
    exchange  = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    return smart_volume_scan(exchange, timeframe=timeframe, limit=limit)

# ── Sentiment & News ──────────────────────────────────────────────────────────

@app.get("/api/sentiment")
def market_sentiment(
    exchange: str = Query("KUCOIN"),
    timeframe: str = Query("1h"),
):
    exchange  = sanitize_exchange(exchange, "KUCOIN")
    timeframe = sanitize_timeframe(timeframe, "1h")
    return analyze_sentiment(exchange, timeframe=timeframe)

@app.get("/api/news")
def financial_news(limit: int = Query(10, ge=1, le=50)):
    return fetch_news_summary(limit=limit)

# ── Yahoo Finance ─────────────────────────────────────────────────────────────

@app.get("/api/price/{ticker}")
def yahoo_price(ticker: str):
    try:
        return get_price(ticker.upper())
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/api/market-snapshot")
def market_snapshot():
    return get_market_snapshot()
