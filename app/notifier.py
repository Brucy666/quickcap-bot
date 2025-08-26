# app/notifier.py
from __future__ import annotations

import os
import aiohttp
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

# -----------------------------
# Discord Webhooks (ENV first)
# -----------------------------
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "").strip()

# ------------- helpers --------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _side_color(side: str) -> int:
    """Discord embed color by side."""
    s = (side or "").upper()
    if s == "LONG":
        return 0x13A10E  # green
    if s == "SHORT":
        return 0xC50F1F  # red
    return 0x2B88D8      # blue default

def _fmt_triggers(trigs: Optional[Iterable[str]]) -> str:
    if not trigs:
        return "—"
    t = [str(x).strip() for x in trigs if str(x).strip()]
    return " • ".join(t) if t else "—"

async def _post_json(url: str, payload: dict) -> None:
    """Low-level poster with minimal noise; safe if url is empty."""
    if not url:
        return
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload) as r:
            if r.status >= 300:
                txt = await r.text()
                raise RuntimeError(f"Discord post failed {r.status}: {txt}")

# ---------------- PUBLIC API ----------------

async def post_signal_embed(
    webhook_url: Optional[str],
    *,
    exchange: str,
    symbol: str,
    interval: str,
    side: str,
    price: float,
    vwap: Optional[float] = None,
    rsi: Optional[float] = None,
    score: Optional[float] = None,
    triggers: Optional[Sequence[str]] = None,
    # basis (spot-perp) extras if present
    basis_pct: Optional[float] = None,
    basis_z: Optional[float] = None,
    title_prefix: str = "QuickCap • Live Signal"
) -> None:
    """
    Send a trading signal embed to Discord.
    If webhook_url is None/empty, falls back to WEBHOOK_LIVE.
    """
    url = webhook_url or WEBHOOK_LIVE

    fields = [
        {"name": "Exchange", "value": str(exchange), "inline": True},
        {"name": "Interval", "value": str(interval), "inline": True},
        {"name": "Side",     "value": str(side),     "inline": True},
        {"name": "Price",    "value": f"{price:.8f}".rstrip("0").rstrip("."), "inline": True},
    ]
    if vwap is not None:
        fields.append({"name": "VWAP", "value": f"{float(vwap):.8f}".rstrip("0").rstrip("."), "inline": True})
    if rsi is not None:
        fields.append({"name": "RSI", "value": f"{float(rsi):.2f}", "inline": True})
    if score is not None:
        fields.append({"name": "Score", "value": f"{float(score):.3f}", "inline": True})
    if basis_pct is not None:
        fields.append({"name": "Basis %", "value": f"{float(basis_pct):.4f}%", "inline": True})
    if basis_z is not None:
        fields.append({"name": "Basis Z", "value": f"{float(basis_z):.3f}", "inline": True})

    embed = {
        "title": f"{title_prefix}",
        "description": f"**{symbol}**",
        "color": _side_color(side),
        "timestamp": _now_iso(),
        "fields": fields + [
            {"name": "Triggers", "value": _fmt_triggers(triggers), "inline": False}
        ],
        "footer": {"text": f"{exchange}:{symbol}:{interval}"},
    }
    await _post_json(url, {"embeds": [embed]})

async def post_backfill_summary(
    venue: str,
    symbol: str,
    interval: str,
    signals: int,
    executions: int,
    outcomes: int,
    *,
    webhook_url: Optional[str] = None
) -> None:
    """
    Short single-line backfill summary to #sniper-backfill (or provided url).
    """
    url = webhook_url or WEBHOOK_BACKFILL
    content = (
        f"✅ **Backfill** `{venue}:{symbol}:{interval}` → "
        f"signals={signals} • executions={executions} • outcomes={outcomes}"
    )
    await _post_json(url, {"content": content[:1990]})

async def post_performance_text(content: str, *, webhook_url: Optional[str] = None) -> None:
    """
    Post pre-formatted performance text (tables/blocks) to #sniper-performance.
    """
    url = webhook_url or WEBHOOK_PERFORMANCE
    if not content:
        return
    # try to wrap in codeblock if not already
    body = content
    if "```" not in content:
        body = f"```\n{content[:1975]}\n```"
    await _post_json(url, {"content": body[:1990]})

async def post_error(msg: str, *, webhook_url: Optional[str] = None) -> None:
    """
    Error line to #sniper-errors.
    """
    url = webhook_url or WEBHOOK_ERRORS
    if not msg:
        return
    await _post_json(url, {"content": f"⚠️ **Error:** {str(msg)[:1960]}"})

# ------------- NOTIFY wrapper (backward-compat) -------------

class _NotifierWrapper:
    """
    Small facade for legacy codepaths that do:
        from app.notifier import NOTIFY
        await NOTIFY.signal(...)
        await NOTIFY.backfill(...)
        await NOTIFY.perf("...")
        await NOTIFY.error("...")

    Methods expect the same kwargs as the helpers above (minus webhook_url).
    """

    async def signal(self, **kwargs) -> None:
        await post_signal_embed(None, **kwargs)

    async def backfill(self, *a, **kw) -> None:
        # supports: (venue, symbol, interval, signals, executions, outcomes)
        await post_backfill_summary(*a, **kw)

    async def perf(self, content: str) -> None:
        await post_performance_text(content)

    async def error(self, msg: str) -> None:
        await post_error(msg)

# Exported singleton for imports: from app.notifier import NOTIFY
NOTIFY = _NotifierWrapper()
