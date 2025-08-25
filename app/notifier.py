# app/notifier.py
from __future__ import annotations

import os
import aiohttp
from datetime import datetime, timezone
from typing import Iterable

# ---- Discord Webhooks (ENV overrides; hardcoded fallbacks kept) ----
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "https://discord.com/api/webhooks/1409631433865302217/BKWwGFqa7vK-l3V1sY5e5aGFq8x0LayqGDYrM6-0OE6xeQC8rFSqMfrAzUFxZeAA1bCJ").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "https://discord.com/api/webhooks/1409631717311909919/wpoF7-XrwJ10eqpo0uo0apJha_nrHgL4iHvi2EWuLy3PFxle71V_sXBDN0tSKsfHaDQA").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "https://discord.com/api/webhooks/1409632131206086708/yTe-T1NcT72UFcY7i33ar-ZITVnrE6DbmPvWla8aek519TZhy--W3mERbH_Vd7z3XJn5").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "https://discord.com/api/webhooks/1409633072097529866/zT3fA34Exzbtn3oLn1-jFu-JY7IO_8cWoBrOxRvcTIZS5nZwUL-V22s5BQntyEsiKvag").strip()

# ---------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _post_json(url: str, payload: dict):
    if not url:
        print("[NO-WEBHOOK] skipped Discord post"); return
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload) as r:
            if r.status >= 300:
                txt = await r.text()
                raise RuntimeError(f"Discord post failed {r.status}: {txt}")

def _fmt_triggers(trigs: Iterable[str] | None) -> str:
    if not trigs: return "—"
    t = [str(x) for x in trigs if str(x).strip()]
    return " • ".join(t) if t else "—"

def _side_color(side: str) -> int:
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F

# ----------------- PUBLIC API (backward compatible) -----------------

async def post_signal_embed(*args, **kwargs):
    """
    Live signal → #sniper-live

    Accepts BOTH:
      - legacy: post_signal_embed(cfg.discord_webhook, exchange=..., symbol=..., interval=..., side=..., price=..., vwap=..., rsi=..., score=..., triggers=[...])
      - new:    post_signal_embed(venue=..., symbol=..., interval=..., side=..., price=..., vwap=..., rsi=..., score=..., triggers=[...], ts=..., signal_type="spot", basis_pct=..., basis_z=...)
    """
    # ignore legacy positional webhook arg if present
    if args and len(args) > 1:
        raise TypeError("post_signal_embed received unexpected positional args")

    # normalize required fields
    venue    = kwargs.pop("venue", None) or kwargs.pop("exchange", None) or "unknown"
    symbol   = kwargs.pop("symbol", "unknown")
    interval = kwargs.pop("interval", "?")
    side     = kwargs.pop("side", "?")
    price    = float(kwargs.pop("price", 0.0))
    vwap     = float(kwargs.pop("vwap", 0.0))
    rsi      = float(kwargs.pop("rsi", 0.0))
    score    = float(kwargs.pop("score", 0.0))
    triggers = kwargs.pop("triggers", [])
    ts       = kwargs.pop("ts", _now_iso())
    sigtype  = kwargs.pop("signal_type", "spot")

    # optional basis extras (show if present)
    basis_pct = kwargs.pop("basis_pct", None)
    basis_z   = kwargs.pop("basis_z", None)

    # silently ignore any other extras to avoid crashing legacy callers
    if kwargs:
        print(f"[WARN] post_signal_embed ignoring extra fields: {list(kwargs.keys())}")

    fields = [
        {"name": "Exchange", "value": venue, "inline": True},
        {"name": "Interval", "value": interval, "inline": True},
        {"name": "Score",    "value": f"{score:0.3f}", "inline": True},
        {"name": "Price",    "value": f"{price}", "inline": True},
        {"name": "VWAP",     "value": f"{vwap}",  "inline": True},
        {"name": "RSI",      "value": f"{rsi:0.2f}", "inline": True},
    ]
    if basis_pct is not None:
        fields.append({"name": "Basis %", "value": f"{float(basis_pct):0.6f}", "inline": True})
    if basis_z is not None:
        fields.append({"name": "Basis Z", "value": f"{float(basis_z):0.3f}", "inline": True})

    embed = {
        "title": f"{symbol} • {side}",
        "description": _fmt_triggers(triggers),
        "color": _side_color(side),
        "timestamp": ts,
        "fields": fields[:25],
        "footer": {"text": f"QuickCap • {sigtype.upper()}"},
    }
    await _post_json(WEBHOOK_LIVE, {"embeds": [embed]})

async def post_backfill_summary(venue: str, symbol: str, interval: str, signals: int, executions: int, outcomes: int):
    """One-line symbol summary after a backfill run → #sniper-backfill."""
    msg = f"✅ **Backfill** `{venue}:{symbol}:{interval}` → signals={signals} • executions={executions} • outcomes={outcomes}"
    await _post_json(WEBHOOK_BACKFILL, {"content": msg[:1990]})

async def post_performance_text(content: str):
    """Plain text performance report → #sniper-performance (rich embed handled by report_to_discord.py)."""
    await _post_json(WEBHOOK_PERFORMANCE, {"content": content[:1990]})

async def post_error(msg: str):
    """Error line → #sniper-errors."""
    await _post_json(WEBHOOK_ERRORS, {"content": f"⚠️ **Error:** {msg}"[:1990]})
