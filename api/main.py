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
import uuid
import urllib.request
from collections import deque
from datetime import datetime, timedelta
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
CONFIG_DEFAULT = REPO_ROOT / "config.json"       # git default (fallback)
SCRIPT_PATH    = REPO_ROOT / "scripts" / "daily_report.py"
ADMIN_HTML     = Path(__file__).parent / "admin.html"

# Persistent config priority (first writable wins):
#   1. /data/config.json  → Railway Volume  (add volume at /data in Railway dashboard)
#   2. /tmp/config.json   → survives restarts within same deployment (wiped on redeploy)
#   3. config.json        → git default (read-only on Railway, used as seed)
_PERSIST_PATHS = [
    Path("/data/config.json"),
    Path("/tmp/config.json"),
]

def _runtime_path() -> Path:
    """Return first writable persistent path."""
    for p in _PERSIST_PATHS:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            # test write
            p.write_text(p.read_text() if p.exists() else "test")
            return p
        except Exception:
            pass
    return Path("/tmp/config.json")  # last resort

# ── Config helpers ─────────────────────────────────────────────────────────────

def _parse_env_config(raw: str) -> dict | None:
    """Parse REPORT_CONFIG_JSON — handles both raw JSON and wrapped {REPORT_CONFIG_JSON: '...'}."""
    try:
        parsed = json.loads(raw)
        # Handle case where user pasted the full API export response
        if isinstance(parsed, dict) and list(parsed.keys()) == ["REPORT_CONFIG_JSON"]:
            inner = parsed["REPORT_CONFIG_JSON"]
            parsed = json.loads(inner) if isinstance(inner, str) else inner
        if isinstance(parsed, dict) and "schedule" in parsed:
            return parsed
    except Exception:
        pass
    return None

def load_config() -> dict:
    # 1. In-memory (current process, cleared on restart)
    if _MEM_CONFIG:
        return dict(_MEM_CONFIG)
    # 2. REPORT_CONFIG_JSON env var (persists across Railway redeploys)
    env_raw = os.environ.get("REPORT_CONFIG_JSON", "")
    if env_raw:
        parsed = _parse_env_config(env_raw)
        if parsed:
            return parsed
    # 3. /tmp file
    for path in _PERSIST_PATHS:
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # 4. git default
    try:
        with open(CONFIG_DEFAULT, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        pass
    return {}

def _railway_update_env(cfg_json: str) -> bool:
    """Auto-update REPORT_CONFIG_JSON env var via Railway GraphQL API."""
    import urllib.request as _ur
    token      = os.environ.get("RAILWAY_API_TOKEN", "")
    project_id = os.environ.get("RAILWAY_PROJECT_ID", "")
    env_id     = os.environ.get("RAILWAY_ENVIRONMENT_ID", "")
    service_id = os.environ.get("RAILWAY_SERVICE_ID", "")
    if not all([token, project_id, env_id, service_id]):
        return False
    query = {
        "query": """
          mutation {
            variableUpsert(input: {
              projectId: %s
              environmentId: %s
              serviceId: %s
              name: "REPORT_CONFIG_JSON"
              value: %s
            })
          }
        """ % (
            json.dumps(project_id),
            json.dumps(env_id),
            json.dumps(service_id),
            json.dumps(cfg_json),
        )
    }
    req = _ur.Request(
        "https://backboard.railway.app/graphql/v2",
        data=json.dumps(query).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        resp = json.loads(_ur.urlopen(req, timeout=10).read())
        return "errors" not in resp
    except Exception:
        return False

def save_config(cfg: dict) -> bool:
    """Save config. Returns True if Railway env var was also updated (persistent)."""
    # 1. In-memory (instant)
    _MEM_CONFIG.clear()
    _MEM_CONFIG.update(cfg)
    # 2. /tmp for subprocess reads
    for path in _PERSIST_PATHS:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2, ensure_ascii=False)
            break
        except Exception:
            pass
    # 3. Git repo root (local dev)
    try:
        with open(CONFIG_DEFAULT, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception:
        pass
    # 4. Railway env var auto-update (persistent across redeploys)
    cfg_json = json.dumps(cfg, ensure_ascii=False)
    os.environ["REPORT_CONFIG_JSON"] = cfg_json  # update current process too
    return _railway_update_env(cfg_json)

# ── In-memory config (survives within same process, cleared on redeploy) ───────
_MEM_CONFIG: dict = {}

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

# ── Alert engine ───────────────────────────────────────────────────────────────

BASE_URL = "https://signal-tradingview-mcp.up.railway.app"
BOT      = "8720452318:AAGgh2WXUW6JFw_Z71eMUBZ0bi-n5eHnwuE"
CHAT     = "5636156156"

_prev_zones: dict[str, str] = {}   # track CDC zone changes

def _tg_send(msg: str) -> None:
    payload = json.dumps({"chat_id": CHAT, "text": msg, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{BOT}/sendMessage",
        data=payload, headers={"Content-Type": "application/json"},
    )
    try: urllib.request.urlopen(req, timeout=10)
    except Exception: pass

def _api_fetch(url: str) -> dict:
    try:
        return json.loads(urllib.request.urlopen(url, timeout=12).read())
    except Exception:
        return {}

def _eval_alert(alert: dict) -> tuple[bool, str]:
    sym   = alert["symbol"].upper()
    exc   = alert.get("exchange", "nasdaq").lower()
    tf    = alert.get("timeframe", "1D")
    atype = alert["type"]
    val   = float(alert.get("value", 0))
    name  = alert.get("name", sym)

    price = rsi = zone = sig = None

    if exc == "yahoo":
        d     = _api_fetch(f"{BASE_URL}/api/price/{sym}")
        price = d.get("price", 0)
    else:
        d     = _api_fetch(f"{BASE_URL}/api/coin/{sym}?exchange={exc}&timeframe={tf}")
        price = (d.get("price_data") or {}).get("current_price", 0)
        rsi   = (d.get("rsi") or {}).get("value")
        sig   = (d.get("market_sentiment") or {}).get("buy_sell_signal")

    # ── Evaluate condition ─────────────────────────────────────────────────────
    if atype == "price_above":
        ok  = bool(price and price >= val)
        msg = f"🔔 <b>Alert: {name}</b>\n💰 <b>{sym}</b> ${price:,.2f} ≥ ${val:,.2f} 🚀\n📈 ราคาแตะเป้าแล้ว!"
    elif atype == "price_below":
        ok  = bool(price and price <= val)
        msg = f"🔔 <b>Alert: {name}</b>\n💰 <b>{sym}</b> ${price:,.2f} ≤ ${val:,.2f} ⚠️\n📉 ราคาลงถึงเป้าแล้ว!"
    elif atype == "rsi_above":
        ok  = rsi is not None and rsi >= val
        msg = f"🔔 <b>Alert: {name}</b>\n📊 <b>{sym}</b> RSI {rsi:.1f} ≥ {val:.0f}\n🔴 Overbought — ระวังการ reverse!"
    elif atype == "rsi_below":
        ok  = rsi is not None and rsi <= val
        msg = f"🔔 <b>Alert: {name}</b>\n📊 <b>{sym}</b> RSI {rsi:.1f} ≤ {val:.0f}\n🟢 Oversold — อาจเป็นจังหวะซื้อ!"
    elif atype == "cdc_change":
        from tradingview_mcp.core.services.cdc_service import analyze_cdc
        cdc_r = analyze_cdc(sym, exc, tf, sig)
        zone  = cdc_r.get("cdc_zone", "")
        key   = f"{sym}_{exc}_{tf}"
        prev  = _prev_zones.get(key, "")
        _prev_zones[key] = zone
        ok    = bool(prev) and prev != zone
        emo   = cdc_r.get("cdc_emoji", "🔄")
        msg   = (f"🔔 <b>Alert: {name}</b>\n"
                 f"{emo} <b>{sym}</b> CDC Zone เปลี่ยน!\n"
                 f"📌 {prev} → <b>{zone}</b>")
    else:
        return False, ""

    return ok, msg

def check_alerts() -> None:
    """Called by APScheduler every 10 minutes."""
    cfg     = load_config()
    alerts  = cfg.get("alerts", [])
    changed = False

    for alert in alerts:
        if not alert.get("active", True):
            continue

        last_fired = alert.get("last_fired")
        repeat     = alert.get("repeat", False)
        cooldown_h = float(alert.get("cooldown_hours", 4))

        # Skip if one-time already fired
        if last_fired and not repeat:
            continue
        # Skip if repeat but still in cooldown
        if last_fired and repeat:
            fired_dt = datetime.fromisoformat(last_fired)
            if datetime.utcnow() - fired_dt < timedelta(hours=cooldown_h):
                continue

        try:
            triggered, msg = _eval_alert(alert)
        except Exception:
            continue

        if triggered:
            _tg_send(msg)
            alert["last_fired"] = datetime.utcnow().isoformat()
            if not repeat:
                alert["active"] = False
            changed = True

    if changed:
        save_config(cfg)

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
    # Alert checker — every 10 minutes, 24/7
    scheduler.add_job(
        check_alerts,
        "interval",
        minutes=10,
        id="alert_checker",
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

@app.get("/api/debug/paths")
def debug_paths():
    import os
    return {
        "cwd": os.getcwd(),
        "repo_root": str(REPO_ROOT),
        "config_default": str(CONFIG_DEFAULT),
        "config_default_exists": CONFIG_DEFAULT.exists(),
        "tmp_config_exists": Path("/tmp/config.json").exists(),
        "data_config_exists": Path("/data/config.json").exists(),
        "mem_config_empty": not bool(_MEM_CONFIG),
        "env_var_set": bool(os.environ.get("REPORT_CONFIG_JSON")),
        "loaded_config_keys": list(load_config().keys()),
    }

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
    railway_ok = save_config(body)
    _apply_schedule(body)
    sch = body.get("schedule", {})
    return {
        "ok": True,
        "persistent": railway_ok,   # True = auto-saved to Railway env var
        "schedule": f"{sch.get('hour',7):02d}:{sch.get('minute',0):02d} {sch.get('timezone','Asia/Bangkok')}",
    }

@app.get("/api/config/export")
def export_config():
    """Return config as minified JSON string — paste into Railway Variables."""
    cfg = load_config()
    return {"REPORT_CONFIG_JSON": json.dumps(cfg, ensure_ascii=False)}

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

# ── Wave 1→2 Bottoming Setup Scanner ──────────────────────────────────────────

@app.get("/api/wave12-scan")
def wave12_scan(
    indices: str = Query("dow30,nasdaq100,sp500", description="Comma-separated: dow30,nasdaq100,sp500"),
):
    """Scan for Wave 1→2 bottoming setups (uses 1y of data — takes ~60s)."""
    from tradingview_mcp.core.services.cdc_scanner_service import (
        scan_wave12_setups, DOW_30, NASDAQ_100, SP_500_EXTRA,
    )
    idx_set  = {s.strip().lower() for s in indices.split(",")}
    universe: list[str] = []
    if "dow30"    in idx_set: universe += DOW_30
    if "nasdaq100" in idx_set: universe += NASDAQ_100
    if "sp500"    in idx_set: universe += SP_500_EXTRA
    universe = sorted(set(universe))
    results  = scan_wave12_setups(symbols=universe, period="1y")
    return results

# ── Alert CRUD endpoints ───────────────────────────────────────────────────────

@app.get("/api/alerts")
def list_alerts():
    return load_config().get("alerts", [])

@app.post("/api/alerts")
def create_alert(body: dict[str, Any] = Body(...)):
    cfg    = load_config()
    alerts = cfg.setdefault("alerts", [])
    body.setdefault("id",             str(uuid.uuid4())[:8])
    body.setdefault("active",         True)
    body.setdefault("last_fired",     None)
    body.setdefault("repeat",         False)
    body.setdefault("cooldown_hours", 4)
    alerts.append(body)
    save_config(cfg)
    return body

@app.delete("/api/alerts/{alert_id}")
def delete_alert(alert_id: str):
    cfg    = load_config()
    alerts = cfg.get("alerts", [])
    before = len(alerts)
    cfg["alerts"] = [a for a in alerts if a.get("id") != alert_id]
    if len(cfg["alerts"]) == before:
        raise HTTPException(status_code=404, detail="Alert not found")
    save_config(cfg)
    return {"ok": True}

@app.patch("/api/alerts/{alert_id}/toggle")
def toggle_alert(alert_id: str):
    cfg = load_config()
    for a in cfg.get("alerts", []):
        if a.get("id") == alert_id:
            a["active"] = not a.get("active", True)
            save_config(cfg)
            return {"id": alert_id, "active": a["active"]}
    raise HTTPException(status_code=404, detail="Alert not found")
