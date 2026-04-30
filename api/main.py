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
from typing import Any, Optional

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
_last_report_date: str = ""   # "YYYY-MM-DD" of last successful trigger

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
    global _running, _last_report_date
    with _run_lock:
        if _running:
            return
        _last_report_date = datetime.utcnow().strftime("%Y-%m-%d")
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

def check_wave12_watchlist() -> None:
    """
    Called every 10 minutes.
    Checks all 'watching' Wave 1→2 (bull) and Wave A→B (bear) items for:

    Bull (direction="bull"):
      - Invalidation : price ≤ w1_start  → 🔴 alert + mark invalid
      - CDC Confirm  : EMA12 crosses UP through EMA26 → 🟢 alert + mark confirmed

    Bear (direction="bear"):
      - Invalidation : price ≥ w1_start  (w1_start = wa_start = top) → 🔴 alert
      - CDC Confirm  : EMA12 crosses DOWN through EMA26 → 🔴🐻 alert + mark confirmed

    Both:
      - Auto-expire  : added > 45 days ago → mark expired
    """
    cfg       = load_config()
    watchlist = cfg.get("wave12_watchlist", [])
    watching  = [w for w in watchlist if w.get("status") == "watching"]
    if not watching:
        return

    symbols = [w["ticker"] for w in watching]

    # ── Batch fetch 1-month closes ─────────────────────────────────────────────
    try:
        import yfinance as yf
        import pandas as pd
        raw = yf.download(
            symbols, period="1mo", interval="1d",
            auto_adjust=True, progress=False, group_by="ticker",
        )
    except Exception:
        return

    def _get_closes(sym: str) -> list[float]:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                lvl0 = raw.columns.get_level_values(0).unique().tolist()
                if sym in lvl0:
                    return raw[sym]["Close"].dropna().values.flatten().tolist()
                return raw["Close"][sym].dropna().values.flatten().tolist()
            return raw["Close"].dropna().values.flatten().tolist()
        except Exception:
            return []

    from tradingview_mcp.core.services.cdc_service import calculate_ema

    changed = False
    for item in watching:
        sym       = item["ticker"]
        w1_start  = float(item["w1_start"])   # bull=bottom, bear=top
        direction = item.get("direction", "bull")
        closes    = _get_closes(sym)
        if not closes:
            continue

        cur = closes[-1]

        # ── 1. Invalidation ───────────────────────────────────────────────────
        if direction == "bull":
            invalidated = cur <= w1_start
            inv_msg = (
                f"🔴 <b>Wave 1→2 INVALID — {sym}</b>\n"
                f"💥 ราคา ${cur:,.2f} ทะลุ bottom ${w1_start:,.2f}\n"
                f"❌ Elliott Wave 2 rule ถูกละเมิด — pattern เสีย!\n"
                f"📅 เพิ่มเมื่อ: {item.get('added_date','')}"
            )
        else:  # bear
            invalidated = cur >= w1_start
            inv_msg = (
                f"🔴 <b>Wave A→B INVALID — {sym}</b>\n"
                f"💥 ราคา ${cur:,.2f} ทะลุ top ${w1_start:,.2f}\n"
                f"❌ Wave B rule ถูกละเมิด — pattern เสีย!\n"
                f"📅 เพิ่มเมื่อ: {item.get('added_date','')}"
            )

        if invalidated:
            _tg_send(inv_msg)
            item["status"]         = "invalid"
            item["invalidated_at"] = datetime.utcnow().isoformat()
            changed = True
            continue

        # ── 2. CDC Confirmation ───────────────────────────────────────────────
        if len(closes) >= 30:
            e12      = calculate_ema(closes, 12)
            e26      = calculate_ema(closes, 26)
            prev_cdc = item.get("cdc_status", "watch")
            fib618   = float(item.get("fib_618", 0))
            fib786   = float(item.get("fib_786", 0))

            if direction == "bull":
                ema_bull = [e12[i] > e26[i] for i in [-3, -2, -1]]
                if ema_bull[-1] and not ema_bull[-2]:    # Fresh cross UP today
                    _tg_send(
                        f"🟢 <b>Wave 1→2 CDC CONFIRMED — {sym}</b>\n"
                        f"✅ EMA12 เพิ่ง cross ขึ้น EMA26!\n"
                        f"💰 ราคา ${cur:,.2f}  |  Bottom: ${w1_start:,.2f}\n"
                        f"🎯 Fib 61.8%: ${fib618:,.2f}  |  78.6%: ${fib786:,.2f}\n"
                        f"📅 เพิ่มเมื่อ: {item.get('added_date','')}"
                    )
                    item["status"]       = "confirmed"
                    item["cdc_status"]   = "fresh_cross"
                    item["confirmed_at"] = datetime.utcnow().isoformat()
                    changed = True
                    continue

                new_cdc = (
                    "fresh_cross"  if ema_bull[-1] and not ema_bull[-2] else
                    "just_crossed" if ema_bull[-1] and not ema_bull[-3] else
                    "bullish"      if ema_bull[-1] else
                    "watch"
                )

            else:  # bear
                ema_bear = [e12[i] < e26[i] for i in [-3, -2, -1]]  # True = EMA12 below EMA26
                if ema_bear[-1] and not ema_bear[-2]:    # Fresh cross DOWN today
                    _tg_send(
                        f"🔴🐻 <b>Wave A→B CDC CONFIRMED — {sym}</b>\n"
                        f"✅ EMA12 เพิ่ง cross ลง EMA26!\n"
                        f"💰 ราคา ${cur:,.2f}  |  Top: ${w1_start:,.2f}\n"
                        f"🎯 Fib 61.8%: ${fib618:,.2f}  |  78.6%: ${fib786:,.2f}\n"
                        f"📅 เพิ่มเมื่อ: {item.get('added_date','')}"
                    )
                    item["status"]       = "confirmed"
                    item["cdc_status"]   = "fresh_cross_down"
                    item["confirmed_at"] = datetime.utcnow().isoformat()
                    changed = True
                    continue

                new_cdc = (
                    "fresh_cross_down"  if ema_bear[-1] and not ema_bear[-2] else
                    "just_crossed_down" if ema_bear[-1] and not ema_bear[-3] else
                    "bearish"           if ema_bear[-1] else
                    "watch_bear"
                )

            if new_cdc != prev_cdc:
                item["cdc_status"] = new_cdc
                changed = True

        # ── 3. Auto-expire after 45 days ──────────────────────────────────────
        added = item.get("added_date", "")
        if added:
            try:
                from datetime import date
                days = (date.today() - date.fromisoformat(added)).days
                if days > 45:
                    item["status"] = "expired"
                    changed = True
            except Exception:
                pass

    if changed:
        save_config(cfg)


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

def _startup_catchup(cfg: dict) -> None:
    """
    If the service restarted during or just after the scheduled report window
    (within 60 minutes of the configured hour), and no report has been sent
    today yet, trigger one immediately so we don't miss a day.
    """
    try:
        from zoneinfo import ZoneInfo
        sch = cfg.get("schedule", {})
        tz_name = sch.get("timezone", "Asia/Bangkok")
        report_hour   = int(sch.get("hour",   6))
        report_minute = int(sch.get("minute", 0))

        tz  = ZoneInfo(tz_name)
        now = datetime.now(tz)
        today_str = now.strftime("%Y-%m-%d")

        # Window: report_time .. report_time + 60 min
        window_start = now.replace(hour=report_hour, minute=report_minute, second=0, microsecond=0)
        window_end   = window_start + timedelta(minutes=60)

        if window_start <= now <= window_end and _last_report_date != today_str:
            print(f"[startup] Catch-up: service started at {now.strftime('%H:%M')} — triggering missed report")
            threading.Thread(target=trigger_report, daemon=True).start()
    except Exception as e:
        print(f"[startup] catch-up check error: {e}")


# ── Telegram Bot Commands ──────────────────────────────────────────────────────

_bot_offset: int = 0


def _handle_bot_command(text: str) -> Optional[str]:
    """
    Parse and execute a Telegram bot command.
    Returns reply string, or None if unrecognised.
    """
    text = text.strip()
    cmd  = text.lower().split()[0] if text else ""

    # /run — trigger daily report
    if cmd == "/run":
        if _running:
            return "⏳ Report กำลังรันอยู่แล้วครับ รอสักครู่..."
        trigger_report()
        return "🚀 เริ่ม Daily Report แล้วครับ!\nจะได้รับ Telegram เร็วๆ นี้ 📱"

    # /status — check today's report status
    if cmd == "/status":
        from zoneinfo import ZoneInfo
        tz  = ZoneInfo("Asia/Bangkok")
        now = datetime.now(tz)
        today_bkk = now.strftime("%Y-%m-%d")
        # Compare with _last_report_date (UTC date, close enough for same-day check)
        sent = "✅ ส่งแล้ววันนี้" if _last_report_date == datetime.utcnow().strftime("%Y-%m-%d") else "❌ ยังไม่ได้ส่งวันนี้"
        running_str = "⏳ กำลังรันอยู่..." if _running else "💤 ไม่ได้รัน"
        job = scheduler.get_job("daily_report")
        nxt = str(job.next_run_time)[:19] if job else "ไม่พบ job"
        log_tail = list(_run_log)[-3:] if _run_log else []
        log_str  = "\n".join(f"  {l}" for l in log_tail) if log_tail else "  (ว่าง)"
        return (
            f"📊 <b>Status</b>\n"
            f"🗓 วันนี้ ({today_bkk}): {sent}\n"
            f"{running_str}\n"
            f"⏰ Next run: {nxt}\n"
            f"📋 Log ล่าสุด:\n{log_str}"
        )

    # /scan TICKER — scan a single stock for all wave setups
    if cmd == "/scan":
        parts = text.split()
        if len(parts) < 2:
            return "📋 Usage: /scan AAPL [1D|1W]\nเช่น /scan AAPL หรือ /scan AAPL 1W"
        sym = parts[1].strip().upper()
        # Optional timeframe: /scan AAPL 1W  (default 1D)
        _TF_MAP = {"1d": "1d", "1w": "1wk", "1wk": "1wk", "1h": "1h"}
        raw_tf  = parts[2].lower() if len(parts) >= 3 else "1d"
        yf_tf   = _TF_MAP.get(raw_tf, "1d")
        tf_label = {"1d": "1D", "1wk": "1W", "1h": "1H"}.get(yf_tf, yf_tf.upper())
        period   = "2y" if yf_tf in ("1d", "1wk") else "3mo"
        try:
            from tradingview_mcp.core.services.cdc_scanner_service import (
                detect_wave12_setup, detect_waveab_setup,
                detect_wave3_setup,  detect_wavec_setup,
                detect_wave45_setup, detect_wave45_bear_setup,
            )
            from tradingview_mcp.core.services.cdc_service import (
                fetch_ohlcv_yahoo, calculate_ema, get_cdc_zone,
            )
            closes = fetch_ohlcv_yahoo(sym, period=period, interval=yf_tf)
            if not closes:
                return f"⚠️ ไม่พบข้อมูล {sym}"

            cur   = closes[-1]
            e12   = calculate_ema(closes, 12)
            e26   = calculate_ema(closes, 26)
            zone  = get_cdc_zone(cur, e12[-1], e26[-1])
            chg1d = (cur - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else 0
            hi52  = max(closes[-252:]) if len(closes) >= 252 else max(closes)
            lo52  = min(closes[-252:]) if len(closes) >= 252 else min(closes)

            lines = [
                f"🔍 <b>Wave Scan: {sym}</b>  [{tf_label}]",
                f"💰 ${cur:,.2f}  ({chg1d:+.2f}% candle ล่าสุด)",
                f"{zone['emoji']} CDC [{tf_label}]: {zone['zone']}  EMA12:{e12[-1]:,.2f} / EMA26:{e26[-1]:,.2f}",
                f"📏 52W  H:${hi52:,.2f}  L:${lo52:,.2f}",
                "",
            ]

            found = False

            r = detect_wave3_setup(closes)
            if r:
                found = True
                lines += [
                    f"🚀 <b>Wave 3 Breakout</b>",
                    f"   +{r['w3_gain_pct']:.1f}% เหนือ W1 peak ${r['w1_peak']:,.2f}",
                    f"   🎯 ${r['ext_162']:,.2f} / ${r['ext_262']:,.2f}  CDC:{r['cdc_status']}",
                    "",
                ]

            r = detect_wave12_setup(closes)
            if r:
                found = True
                lines += [
                    f"🐂 <b>Wave 1→2 Setup</b>",
                    f"   W2 retrace {r['retrace_pct']}% {r['fib_label']}",
                    f"   bot:${r['w1_start']:,.2f}  peak:${r['w1_peak']:,.2f}  CDC:{r['cdc_status']}",
                    "",
                ]

            r = detect_wave45_setup(closes)
            if r:
                found = True
                lines += [
                    f"⚡ <b>Wave 4→5 Bull</b>",
                    f"   W4 pullback {r['pullback_pct']:.1f}% {r['fib_label']}  run:+{r['total_run_pct']:.1f}%",
                    f"   🎯 ${r['w5_target_min']:,.2f} / ${r['w5_target_std']:,.2f}  CDC:{r['cdc_status']}",
                    "",
                ]

            r = detect_wavec_setup(closes)
            if r:
                found = True
                lines += [
                    f"📉 <b>Wave C Breakdown</b>",
                    f"   -{r['wc_drop_pct']:.1f}% ต่ำกว่า WA low ${r['wa_bottom']:,.2f}",
                    f"   🎯 ${r['ext_162']:,.2f} / ${r['ext_262']:,.2f}  CDC:{r['cdc_status']}",
                    "",
                ]

            r = detect_waveab_setup(closes)
            if r:
                found = True
                lines += [
                    f"🐻 <b>Wave A→B Setup</b>",
                    f"   WB retrace {r['retrace_pct']}% {r['fib_label']}",
                    f"   top:${r['wa_start']:,.2f}  WA low:${r['wa_bottom']:,.2f}  CDC:{r['cdc_status']}",
                    "",
                ]

            r = detect_wave45_bear_setup(closes)
            if r:
                found = True
                lines += [
                    f"⚡ <b>Wave 4→5 Bear</b>",
                    f"   W4 bounce {r['bounce_pct']:.1f}% {r['fib_label']}  drop:-{r['total_drop_pct']:.1f}%",
                    f"   🎯 ${r['w5_target_min']:,.2f} / ${r['w5_target_std']:,.2f}  CDC:{r['cdc_status']}",
                    "",
                ]

            if not found:
                lines.append("📭 ไม่พบ wave setup ที่ match criteria")
                lines.append("(ต้องการ downtrend/uptrend ≥20% + bounce/drop ≥10%)")

            return "\n".join(lines)
        except Exception as e:
            return f"⚠️ Error scanning {sym}: {str(e)[:120]}"

    # /help
    if cmd == "/help":
        return (
            "🤖 <b>TradingView Bot Commands</b>\n\n"
            "/run — รัน Daily Report ทันที\n"
            "/status — เช็คสถานะ report วันนี้\n"
            "/scan AAPL — scan wave setup 1 ตัว\n"
            "/help — แสดงคำสั่งทั้งหมด"
        )

    return None  # unknown command


def _telegram_bot_loop() -> None:
    """
    Long-poll Telegram getUpdates in background thread.
    Responds only to CHAT (authorised chat ID).
    On startup, fast-forward offset to skip old messages (prevent duplicate replies after redeploy).
    """
    global _bot_offset
    import time
    import urllib.parse

    # ── Skip messages that arrived before this process started ────────────────
    try:
        url  = f"https://api.telegram.org/bot{BOT}/getUpdates?limit=1&offset=-1"
        resp = json.loads(urllib.request.urlopen(url, timeout=10).read())
        updates = resp.get("result", [])
        if updates:
            _bot_offset = updates[-1]["update_id"] + 1
            print(f"[bot] fast-forward offset to {_bot_offset}")
    except Exception as e:
        print(f"[bot] offset init error: {e}")

    while True:
        try:
            params = urllib.parse.urlencode({"offset": _bot_offset, "timeout": 30})
            url    = f"https://api.telegram.org/bot{BOT}/getUpdates?{params}"
            resp   = json.loads(urllib.request.urlopen(url, timeout=35).read())
            for update in resp.get("result", []):
                _bot_offset = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "")
                chat_id = str(msg.get("chat", {}).get("id", ""))

                if not text.startswith("/"):
                    continue
                if chat_id != CHAT:
                    continue  # security: only authorised chat

                reply = _handle_bot_command(text)
                if reply:
                    _tg_send(reply)
        except Exception as e:
            print(f"[bot] poll error: {e}")
            time.sleep(5)


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
    # Wave 1→2 watchlist checker — every 10 minutes, 24/7
    scheduler.add_job(
        check_wave12_watchlist,
        "interval",
        minutes=10,
        id="wave12_checker",
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
    # ── Catch-up: if service restarted during the report window, run now ──────
    _startup_catchup(cfg)
    # ── Telegram bot command listener (long-poll in background) ───────────────
    threading.Thread(target=_telegram_bot_loop, daemon=True).start()

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

@app.get("/api/wave12-watchlist")
def get_wave12_watchlist():
    return load_config().get("wave12_watchlist", [])

@app.post("/api/wave12-watchlist/sync")
def sync_wave12_watchlist(body: list[dict] = Body(...)):
    """
    Called by daily_report.py after the morning scan.
    Upserts new setups into the watchlist (preserves existing items & status).
    Handles both bull (Wave 1→2) and bear (Wave A→B) directions.
    Key: (ticker, direction) — same ticker can appear in both bull and bear lists.
    Returns how many were added.
    """
    cfg       = load_config()
    watchlist = cfg.setdefault("wave12_watchlist", [])
    # Key by (ticker, direction) to allow same stock in both bull and bear lists
    existing  = {
        (w["ticker"], w.get("direction", "bull"))
        for w in watchlist if w.get("status") == "watching"
    }
    added = 0
    for r in body:
        sym       = r.get("ticker", "")
        direction = r.get("direction", "bull")
        if not sym:
            continue
        key = (sym, direction)
        if key in existing:
            # Update live fields for existing watching items
            for w in watchlist:
                if w["ticker"] == sym and w.get("direction", "bull") == direction and w.get("status") == "watching":
                    w["cdc_status"]    = r.get("cdc_status",    w.get("cdc_status"))
                    w["retrace_pct"]   = r.get("retrace_pct",   w.get("retrace_pct"))
                    w["current_price"] = r.get("current_price", w.get("current_price"))
        else:
            if direction == "bull":
                entry = {
                    "id":             str(uuid.uuid4())[:8],
                    "direction":      "bull",
                    "ticker":         sym,
                    "w1_start":       r.get("w1_start", 0),       # bottom
                    "w1_peak":        r.get("w1_peak",  0),       # Wave 1 peak
                    "fib_618":        r.get("fib_618",  0),
                    "fib_786":        r.get("fib_786",  0),
                    "wave1_gain_pct": r.get("wave1_gain_pct", 0),
                    "downtrend_pct":  r.get("downtrend_pct",  0),
                    "retrace_pct":    r.get("retrace_pct",    0),
                    "fib_label":      r.get("fib_label", ""),
                    "current_price":  r.get("current_price", 0),
                    "cdc_status":     r.get("cdc_status", "watch"),
                    "added_date":     datetime.utcnow().date().isoformat(),
                    "status":         "watching",
                }
            else:  # bear
                entry = {
                    "id":              str(uuid.uuid4())[:8],
                    "direction":       "bear",
                    "ticker":          sym,
                    "w1_start":        r.get("w1_start",  r.get("wa_start", 0)),  # top (invalidation level)
                    "w1_peak":         r.get("wa_bottom", r.get("w1_peak",  0)),  # Wave A bottom
                    "fib_618":         r.get("fib_618",   0),
                    "fib_786":         r.get("fib_786",   0),
                    "wavea_drop_pct":  r.get("wavea_drop_pct", 0),
                    "uptrend_pct":     r.get("uptrend_pct",    0),
                    "retrace_pct":     r.get("retrace_pct",    0),
                    "fib_label":       r.get("fib_label", ""),
                    "current_price":   r.get("current_price", 0),
                    "cdc_status":      r.get("cdc_status", "watch_bear"),
                    "added_date":      datetime.utcnow().date().isoformat(),
                    "status":          "watching",
                }
            watchlist.append(entry)
            added += 1
    save_config(cfg)
    return {"added": added, "total_watching": len([w for w in watchlist if w.get("status") == "watching"])}

@app.delete("/api/wave12-watchlist/{item_id}")
def delete_wave12_item(item_id: str):
    cfg       = load_config()
    watchlist = cfg.get("wave12_watchlist", [])
    before    = len(watchlist)
    cfg["wave12_watchlist"] = [w for w in watchlist if w.get("id") != item_id]
    if len(cfg["wave12_watchlist"]) == before:
        raise HTTPException(status_code=404, detail="Item not found")
    save_config(cfg)
    return {"ok": True}

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
