# app/notifier.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable, Mapping, Any, Optional

import aiohttp

# ----------------------------
# Discord webhooks (ENV first)
# ----------------------------
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "").strip()


# ----------------------------
# Small utilities
# ----------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _join_triggers(trigs: Optional[Iterable[str]]) -> str:
    if not trigs:
        return "—"
    items = [str(t).strip() for t in trigs if str(t).strip()]
    return " • ".join(items) if items else "—"

def _side_color(side: str) -> int:
    # green long / red short
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F

async def _post_json(url: str, payload: Mapping[str, Any]) -> None:
    """
    Fire-and-forget style post to a Discord webhook.
    Creates a short-lived session per call (keeps the code simple/safe).
    """
    if not url:
        # Running without a webhook is fine in dev; just skip silently.
        return
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload) as r:
            if r.status >= 300:
                txt = await r.text()
                raise RuntimeError(f"Discord post failed {r.status}: {txt}")


# ----------------------------
# Public low-level senders
# ----------------------------
async def post_signal_embed(
    _unused: Optional[str] = None,  # kept for backwards compatibility (used to pass a webhook)
    *,
    exchange: str,
    symbol: str,
    interval: str = "1m",
    side: str,
    price: float,
    vwap: Optional[float] = None,
    rsi: Optional[float] = None,
    score: Optional[float] = None,
    triggers: Optional[Iterable[str]] = None,
    # optional basis fields (when you send BASIS alerts)
    basis_pct: Optional[float] = None,
    basis_z: Optional[float] = None,
) -> None:
    """
    Send a live signal embed to #sniper-live (or your WEBHOOK_LIVE).
    """
    title = f"{symbol} • {side.upper()}"
    description = _join_triggers(triggers)

    fields = [
        {"name": "Exchange", "value": str(exchange), "inline": True},
        {"name": "Interval", "value": str(interval), "inline": True},
    ]

    if price is not None:
        fields.append({"name": "Price", "value": f"{price}", "inline": True})
    if vwap is not None:
        fields.append({"name": "VWAP", "value": f"{vwap}", "inline": True})
    if rsi is not None:
        fields.append({"name": "RSI", "value": f"{rsi}", "inline": True})
    if score is not None:
        fields.append({"name": "Score", "value": f"{score}", "inline": True})

    # basis extras (only shown when provided)
    if basis_pct is not None:
        fields.append({"name": "Basis %", "value": f"{basis_pct:.4f}", "inline": True})
    if basis_z is not None:
        fields.append({"name": "Basis Z", "value": f"{basis_z:.3f}", "inline": True})

    embed = {
        "title": title,
        "description": description,
        "color": _side_color(side),
        "timestamp": _now_iso(),
        "fields": fields,
        "footer": {"text": f"{exchange}:{symbol}:{interval}"},
    }

    await _post_json(WEBHOOK_LIVE, {"embeds": [embed]})


async def post_backfill_summary(
    venue: str,
    symbol: str,
    interval: str,
    signals: int,
    executions: int,
    outcomes: int,
) -> None:
    content = (
        f"✅ **Backfill** `{venue}:{symbol}:{interval}` → "
        f"signals={signals} • executions={executions} • outcomes={outcomes}"
    )
    await _post_json(WEBHOOK_BACKFILL, {"content": content[:1990]})


async def post_performance_text(content: str) -> None:
    """
    Send a plain text/table performance block to #sniper-performance.
    """
    await _post_json(WEBHOOK_PERFORMANCE, {"content": content[:1990]})


async def post_error(msg: str) -> None:
    await _post_json(WEBHOOK_ERRORS, {"content": f"⚠️ **Error:** {msg}"[:1990]})


# ---------------------------------------------------
# High-level NOTIFY wrapper (new API + legacy aliases)
# ---------------------------------------------------
class _NotifierWrapper:
    """
    Facade used everywhere as NOTIFY.
    Exposes:
      - signal(), backfill(), perf(), error()   (new, concise API)
      - post_signal_embed(), post_backfill_summary(), post_performance_text(), post_error() (legacy aliases)
    """

    # ---------- new / preferred ----------
    async def signal(self, **kwargs) -> None:
        # forwards all keyword args to post_signal_embed
        await post_signal_embed(None, **kwargs)

    async def backfill(self, *args, **kwargs) -> None:
        await post_backfill_summary(*args, **kwargs)

    async def perf(self, content: str) -> None:
        await post_performance_text(content)

    async def error(self, msg: str) -> None:
        await post_error(msg)

    # ---------- legacy aliases ----------
    async def post_signal_embed(self, **kwargs) -> None:
        await self.signal(**kwargs)

    async def post_backfill_summary(self, *args, **kwargs) -> None:
        await self.backfill(*args, **kwargs)

    async def post_performance_text(self, content: str) -> None:
        await self.perf(content)

    async def post_error(self, msg: str) -> None:
        await self.error(msg)


# Export a singleton used throughout the app
NOTIFY = _NotifierWrapper()
