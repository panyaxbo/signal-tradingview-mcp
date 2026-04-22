"""
TradingAgents + DeepSeek Integration
-------------------------------------
Patches TradingAgents at runtime to add DeepSeek provider support,
then runs multi-agent analysis to produce a rich BUY/SELL/HOLD report
with Bull/Bear reasoning for use in Telegram daily reports.
"""
from __future__ import annotations

import os
import sys
import site


# ── Runtime patcher ────────────────────────────────────────────────────────────

def _find_tradingagents_base() -> str | None:
    """Find tradingagents package directory from any site-packages."""
    search = []
    try:
        search += site.getsitepackages()
    except AttributeError:
        pass
    try:
        search.append(site.getusersitepackages())
    except AttributeError:
        pass
    for d in search:
        candidate = os.path.join(d, "tradingagents")
        if os.path.isdir(candidate):
            return candidate
    return None


def patch_deepseek() -> bool:
    """
    Patch TradingAgents package files to add DeepSeek provider support.
    Safe to call multiple times — checks before writing.
    Returns True if any file was changed.
    """
    base = _find_tradingagents_base()
    if not base:
        return False

    changed = False

    # 1. default_config.py — add DEEPSEEK to LLMProvider StrEnum
    cfg_path = os.path.join(base, "default_config.py")
    if os.path.exists(cfg_path):
        txt = open(cfg_path).read()
        if "DEEPSEEK" not in txt:
            for old, new in [
                ('XAI = "xai"', 'XAI = "xai"\n    DEEPSEEK = "deepseek"'),
                ("XAI = 'xai'", "XAI = 'xai'\n    DEEPSEEK = 'deepseek'"),
            ]:
                if old in txt:
                    open(cfg_path, "w").write(txt.replace(old, new))
                    changed = True
                    break

    # 2. factory.py — add "deepseek" to the OpenAI-compatible list
    fac_path = os.path.join(base, "llm_clients", "factory.py")
    if os.path.exists(fac_path):
        txt = open(fac_path).read()
        if '"deepseek"' not in txt and "'deepseek'" not in txt:
            for old, new in [
                ('"xai")', '"xai", "deepseek")'),
                ("'xai')", "'xai', 'deepseek')"),
            ]:
                if old in txt:
                    open(fac_path, "w").write(txt.replace(old, new))
                    changed = True
                    break

    # 3. openai_client.py — add DeepSeek base_url block
    cli_path = os.path.join(base, "llm_clients", "openai_client.py")
    if os.path.exists(cli_path):
        txt = open(cli_path).read()
        if "deepseek" not in txt.lower():
            ds_block = (
                '        elif self.provider == "deepseek":\n'
                '            llm_kwargs["base_url"] = "https://api.deepseek.com/v1"\n'
                '            api_key = os.environ.get("DEEPSEEK_API_KEY")\n'
                '            if api_key:\n'
                '                llm_kwargs["api_key"] = api_key\n'
                '        '
            )
            for marker in [
                '        elif self.provider == "openai":',
                "        elif self.provider == 'openai':",
            ]:
                if marker in txt:
                    open(cli_path, "w").write(txt.replace(marker, ds_block + marker.lstrip()))
                    changed = True
                    break

    # Flush Python's module cache so patched files are re-imported
    if changed:
        for key in list(sys.modules.keys()):
            if "tradingagents" in key:
                del sys.modules[key]

    return changed


# ── Text helpers ───────────────────────────────────────────────────────────────

import re as _re


def _first_sentence(text: str, max_chars: int = 140) -> str:
    """Return the first meaningful sentence (≤ max_chars) from a text block."""
    if not text:
        return ""
    text = text.strip()
    for sep in [". ", ".\n", "\n\n", "\n"]:
        idx = text.find(sep)
        if 5 < idx <= max_chars:
            return text[: idx + 1].strip()
    return text[:max_chars].strip()


def _clean_md(text: str) -> str:
    """Strip common markdown syntax for plain Telegram text."""
    text = _re.sub(r"\*+", "", text)   # **bold**
    text = _re.sub(r"#+\s*", "", text) # ## headings
    text = _re.sub(r"`+", "", text)    # `code`
    return text.strip()


def _extract_key_points(
    final_decision: str,
    bull_history: str,
    bear_history: str,
) -> tuple[str, str, str]:
    """
    Extract (bull_point, bear_point, final_text) from TradingAgents output.

    Prefers structured bullet points from final_trade_decision;
    falls back to the first meaningful sentence in the history strings.
    """
    bull_point = ""
    bear_point = ""
    final_text = ""

    if final_decision:
        lines = final_decision.split("\n")

        # Extract the top-level recommendation headline
        for line in lines[:6]:
            cleaned = _clean_md(line)
            if cleaned and any(k in cleaned.upper() for k in ("HOLD", "BUY", "SELL")):
                final_text = cleaned[:200]
                break

        # Walk lines looking for bull / bear bullet points
        in_bull = False
        in_bear = False
        for line in lines:
            ll = line.lower()
            if any(k in ll for k in ("aggressive analyst", "buy/add", "bull")):
                in_bull, in_bear = True, False
            elif any(k in ll for k in ("conservative analyst", "sell/trim", "bear", "risk analyst")):
                if in_bull:          # only switch once we've passed the bull section
                    in_bull, in_bear = False, True

            stripped = line.strip()
            if stripped.startswith("- ") and len(stripped) > 12:
                point = _clean_md(stripped[2:])[:140]
                if in_bull and not bull_point:
                    bull_point = point
                elif in_bear and not bear_point:
                    bear_point = point

            if bull_point and bear_point:
                break

    # Fallback: parse first meaningful sentence from history strings
    def _from_history(hist: str) -> str:
        if not hist:
            return ""
        # Skip role prefix ("Bull Analyst: Alright, ...")
        for prefix in ("Bull Analyst:", "Bear Analyst:", "Alright,", "Let me "):
            idx = hist.find(prefix)
            if 0 <= idx < 30:
                hist = hist[idx + len(prefix):].strip()
        return _first_sentence(hist, 130)

    if not bull_point:
        bull_point = _from_history(bull_history)
    if not bear_point:
        bear_point = _from_history(bear_history)

    return bull_point, bear_point, final_text


def _confidence_pct(ai_decision: str, combined_signal: str, combined_conf: str) -> int:
    """
    Derive a rough confidence % from the AI decision vs combined TV+CDC signal.

    - AI=BUY/SELL aligned with combined HIGH  → 82%
    - AI=BUY/SELL aligned with combined MEDIUM → 65%
    - AI=BUY/SELL aligned with combined LOW   → 48%
    - AI=HOLD (neutral)                        → 55%
    - AI conflicts with combined direction     → 35%
    """
    if ai_decision == "HOLD":
        return 55

    ai_buy  = ai_decision == "BUY"
    ai_sell = ai_decision == "SELL"
    c_buy   = "BUY"  in combined_signal
    c_sell  = "SELL" in combined_signal

    if (ai_buy and c_buy) or (ai_sell and c_sell):
        return {"HIGH": 82, "MEDIUM": 65, "LOW": 48}.get(combined_conf, 60)
    if (ai_buy and c_sell) or (ai_sell and c_buy):
        return 35
    return 52


# ── Main analysis function ─────────────────────────────────────────────────────

def analyze_with_ta(
    ta_ticker: str,
    date: str,
    combined_signal: str = "NEUTRAL",
    combined_conf: str = "MEDIUM",
) -> dict:
    """
    Run TradingAgents with DeepSeek and return a structured result dict.

    Args:
        ta_ticker:       Yahoo Finance-compatible ticker (e.g. 'AAPL', 'BTC-USD', 'GLD')
        date:            Analysis date 'YYYY-MM-DD'
        combined_signal: TV+CDC combined signal string (for confidence calc)
        combined_conf:   Confidence level 'HIGH'|'MEDIUM'|'LOW'

    Returns dict with keys:
        decision, confidence_pct, bull_point, bear_point, final_text
    """
    patch_deepseek()

    from tradingagents.graph.trading_graph import TradingAgentsGraph
    from tradingagents.default_config import TradingAgentsConfig, LLMProvider

    config = TradingAgentsConfig(
        llm_provider=LLMProvider.DEEPSEEK,
        deep_think_llm="deepseek-reasoner",
        quick_think_llm="deepseek-chat",
        max_debate_rounds=1,
        max_risk_discuss_rounds=1,
        max_recur_limit=100,
    )

    ta  = TradingAgentsGraph(debug=False, config=config)
    state, decision = ta.propagate(ta_ticker, date)
    dec = str(decision).strip().upper()

    bull_history    = ""
    bear_history    = ""
    final_decision  = ""

    try:
        inv          = state.investment_debate_state
        bull_history = inv.bull_history or ""
        bear_history = inv.bear_history or ""
    except Exception:
        pass

    try:
        final_decision = state.final_trade_decision or ""
    except Exception:
        pass

    bull_point, bear_point, final_text = _extract_key_points(
        final_decision, bull_history, bear_history
    )
    conf_pct = _confidence_pct(dec, combined_signal, combined_conf)

    return {
        "decision":       dec,
        "confidence_pct": conf_pct,
        "bull_point":     bull_point,
        "bear_point":     bear_point,
        "final_text":     final_text,
    }


# ── Telegram formatter ─────────────────────────────────────────────────────────

DECISION_EMOJI = {"BUY": "✅", "SELL": "❌", "HOLD": "⏸"}


def format_ta_block(
    label: str,
    emoji: str,
    tv_signal: str | None,
    cdc_zone: str,
    cdc_emoji: str,
    combined_signal: str,
    combined_sig_emoji: str,
    ta_result: dict,
) -> str:
    """
    Build the Telegram HTML block for one symbol's deep analysis.

    Example output:
        🍎 AI ANALYSIS — AAPL

        📊 TV Signal: BUY
        🔵 CDC Zone: Strong Bull
        ✅ Combined: CONFIRMED BUY

        🤖 AI Consensus: BUY (82%)
        🟢 Bull: RSI recovery with volume surge...
        🔴 Bear: Approaching key resistance zone...
        📝 Buy on pullbacks toward the 200-EMA...
    """
    dec      = ta_result["decision"]
    dec_e    = DECISION_EMOJI.get(dec, "❓")
    conf_pct = ta_result["confidence_pct"]

    lines = [
        f"{emoji} <b>AI ANALYSIS — {label}</b>",
        "",
        f"📊 TV Signal: {tv_signal or 'N/A'}",
        f"{cdc_emoji} CDC Zone: {cdc_zone}",
        f"{combined_sig_emoji} Combined: {combined_signal}",
        "",
        f"🤖 AI Consensus: {dec_e} <b>{dec}</b> ({conf_pct}%)",
    ]

    if ta_result.get("bull_point"):
        lines.append(f"🟢 Bull: {ta_result['bull_point']}")
    if ta_result.get("bear_point"):
        lines.append(f"🔴 Bear: {ta_result['bear_point']}")
    if ta_result.get("final_text"):
        lines.append(f"📝 {ta_result['final_text']}")

    return "\n".join(lines)
