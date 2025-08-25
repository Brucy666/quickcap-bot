# app/notifier.py
from __future__ import annotations

import os
import aiohttp
from datetime import datetime, timezone
from typing import Iterable, Optional

# ---- Channel webhooks (ENV first; fallback to provided defaults) ----
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

def _fmt_triggers(trigs: Iterable[str]) -> str:
    t = [str(x) for x in trigs if str(x).strip()]
    return " • ".join(t) if t else "—"

def _side_color(side: str) -> int:
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F

# ----------------- PUBLIC API -----------------

async def post_signal_embed(
    webhook_url: Optional[str] = None,   # kept for compatibility
    *,
    exchange: str,
    symbol: str,
    interval: str,
    side: str,
    price: float,
    vwap: float,
    rsi: float,
    score: float,
    triggers: Iterable[str],
    **extras,
):
    """
    Post a live signal embed to #sniper-live.
    """
    url = (webhook_url or WEBHOOK_LIVE).strip()
    embed = {
        "title": f"{symbol} • {side}",
        "description": _fmt_triggers(triggers),
        "color": _side_color(side),
        "timestamp": _now_iso(),
        "fields": [
            {"name": "Exchange", "value": exchange, "inline": True},
            {"name": "Interval", "value": interval, "inline": True},
            {"name": "Score", "value": f"{score:0.3f}", "inline": True},
            {"name": "Price", "value": f"{price}", "inline": True},
            {"name": "VWAP",  "value": f"{vwap}",  "inline": True},
            {"name": "RSI",   "value": f"{rsi:0.2f}", "inline": True},
        ],
        "footer": {"text": "QuickCap • Live Signal"},
    }
    await _post_json(url, {"embeds": [embed]})

async def post_backfill_summary(
    venue: str,
    symbol: str,
    interval: str,
    signals: int,
    executions: int,
    outcomes: int
):
    """
    One-line summary per symbol after a backfill run → #sniper-backfill.
    """
    msg = f"✅ **Backfill** `{venue}:{symbol}:{interval}` → signals={signals} • executions={executions} • outcomes={outcomes}"
    await _post_json(WEBHOOK_BACKFILL, {"content": msg[:1990]})

async def post_performance_text(content: str):
    """
    Multi-table performance report (winrate/expectancy/MFE/MAE) → #sniper-performance.
    """
    await _post_json(WEBHOOK_PERFORMANCE, {"content": content[:1990]})

async def post_error(msg: str):
    """
    Error line → #sniper-errors.
    """
    await _post_json(WEBHOOK_ERRORS, {"content": f"⚠️ **Error:** {msg}"[:1990]})
