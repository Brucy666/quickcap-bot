# app/notifier.py
from __future__ import annotations
import os
import aiohttp
import asyncio
from datetime import datetime, timezone
from typing import Iterable, Optional, Dict, Any, List

from app.logger import get_logger

log = get_logger("notifier")

# ---- Channel webhooks (ENV first; fallback to provided defaults) ----
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "").strip()

# single shared session for this module
_session: Optional[aiohttp.ClientSession] = None

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _ensure_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session

async def _post_json(url: str, payload: Dict[str, Any]) -> None:
    """Fire-and-forget safe post (logs clean errors, never dumps aiohttp objects)."""
    if not url:
        # allow running without Discord configured
        return
    try:
        sess = await _ensure_session()
        async with sess.post(url, json=payload, timeout=10) as resp:
            if resp.status >= 300:
                text = await resp.text()
                log.error(f"[DISCORD] post failed {resp.status}: {text[:400]}")
    except Exception as e:
        log.exception(f"[DISCORD] post error: {e}")

def _fmt_triggers(trigs: Iterable[str]) -> str:
    t = [str(x).strip() for x in (trigs or []) if str(x).strip()]
    return ", ".join(t) if t else "—"

def _side_color(side: str) -> int:
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F

def _clip(s: Any, n: int) -> str:
    if s is None:
        return ""
    s = str(s)
    return (s[: n - 1] + "…") if len(s) > n else s

# ----------------- PUBLIC API -----------------

async def post_signal_embed(
    webhook: Optional[str],
    *,
    exchange: str,
    symbol: str,
    interval: str,
    side: str,
    price: float,
    vwap: Optional[float] = None,
    rsi: Optional[float] = None,
    score: Optional[float] = None,
    triggers: Optional[List[str]] = None,
    basis_pct: Optional[float] = None,
    basis_z: Optional[float] = None,
) -> None:
    """
    Send a trading signal embed to Discord.
    Accepts optional basis fields for spot-perp signals.
    If `webhook` is None, uses WEBHOOK_LIVE.
    """
    url = (webhook or WEBHOOK_LIVE or "").strip()
    title = f"{symbol} • {side.upper()}"
    desc_lines = [f"`{exchange}:{interval}`  •  {_fmt_triggers(triggers)}"]
    if basis_pct is not None:
        desc_lines.append(f"Basis: **{basis_pct:.4f}%**  Z: **{basis_z:.2f}**")
    desc = "\n".join(desc_lines)

    fields = [
        {"name": "Price", "value": f"{price:.6g}", "inline": True},
    ]
    if vwap is not None:
        fields.append({"name": "VWAP", "value": f"{vwap:.6g}", "inline": True})
    if rsi is not None:
        fields.append({"name": "RSI", "value": f"{float(rsi):.2f}", "inline": True})
    if score is not None:
        fields.append({"name": "Score", "value": f"{float(score):.3f}", "inline": True})

    embed = {
        "title": _clip(title, 240),
        "description": _clip(desc, 1000),
        "color": _side_color(side),
        "timestamp": _now_iso(),
        "fields": fields[:25],  # Discord max fields
        "footer": {"text": "QuickCap • Live Signal"},
    }

    await _post_json(url, {"embeds": [embed]})

async def post_backfill_summary(
    venue: str,
    symbol: str,
    interval: str,
    signals: int,
    executions: int,
    outcomes: int,
) -> None:
    """Text summary → #sniper-backfill."""
    msg = (
        f"✅ **Backfill** `{venue}:{symbol}:{interval}` → "
        f"signals={signals} • executions={executions} • outcomes={outcomes}"
    )
    await _post_json(WEBHOOK_BACKFILL, {"content": _clip(msg, 1990)})

async def post_performance_text(content: str) -> None:
    """Tables/metrics → #sniper-performance (splits long text automatically)."""
    if not WEBHOOK_PERFORMANCE:
        return
    # Discord message hard cap ~= 2000 chars
    text = str(content or "")
    chunks = []
    while text:
        chunk = text[:1900]
        text = text[1900:]
        chunks.append(chunk)
    for i, ch in enumerate(chunks, 1):
        suffix = f"\n`[{i}/{len(chunks)}]`" if len(chunks) > 1 else ""
        await _post_json(WEBHOOK_PERFORMANCE, {"content": ch + suffix})

async def post_error(msg: str) -> None:
    """Errors → #sniper-errors."""
    await _post_json(WEBHOOK_ERRORS, {"content": _clip(f"⚠️ **Error:** {msg}", 1990)})

# Optional: call at shutdown to close aiohttp cleanly
async def close_notifier_session() -> None:
    global _session
    try:
        if _session and not _session.closed:
            await _session.close()
    finally:
        _session = None
