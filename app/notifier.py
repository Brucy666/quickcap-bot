# app/notifier.py
from __future__ import annotations
import os, aiohttp
from datetime import datetime, timezone
from typing import Iterable

# ---- Discord Webhooks ----
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "https://discord.com/api/webhooks/1409631433865302217/BKWwGFqa7vK-l3V1sY5e5aGFq8x0LayqGDYrM6-0OE6xeQC8rFSqMfrAzUFxZeAA1bCJ").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "https://discord.com/api/webhooks/1409631717311909919/wpoF7-XrwJ10eqpo0uo0apJha_nrHgL4iHvi2EWuLy3PFxle71V_sXBDN0tSKsfHaDQA").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "https://discord.com/api/webhooks/1409632131206086708/yTe-T1NcT72UFcY7i33ar-ZITVnrE6DbmPvWla8aek519TZhy--W3mERbH_Vd7z3XJn5").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "https://discord.com/api/webhooks/1409633072097529866/zT3fA34Exzbtn3oLn1-jFu-JY7IO_8cWoBrOxRvcTIZS5nZwUL-V22s5BQntyEsiKvag").strip()

# ---- Helpers ----
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _post_json(url: str, payload: dict):
    if not url:
        print("[NO-WEBHOOK] skipped Discord post")
        return
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload) as r:
            if r.status >= 300:
                raise RuntimeError(f"Discord post failed {r.status}: {await r.text()}")

def _fmt_triggers(trigs: Iterable[str]) -> str:
    t = [str(x) for x in trigs if str(x).strip()]
    return " • ".join(t) if t else "—"

def _side_color(side: str) -> int:
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F

# ---- Public API ----
async def post_signal_embed(exchange: str, symbol: str, interval: str, side: str,
                            price: float, vwap: float, rsi: float, score: float,
                            triggers: Iterable[str], basis_pct: float | None = None, basis_z: float | None = None):
    """Send live trade signals → #sniper-live"""
    embed = {
        "title": f"{symbol} • {side}",
        "description": _fmt_triggers(triggers),
        "color": _side_color(side),
        "fields": [
            {"name": "Exchange", "value": exchange, "inline": True},
            {"name": "Interval", "value": interval, "inline": True},
            {"name": "Score", "value": f"{score:.3f}", "inline": True},
            {"name": "Price", "value": f"{price:.5f}", "inline": True},
            {"name": "VWAP", "value": f"{vwap:.5f}", "inline": True},
            {"name": "RSI", "value": f"{rsi:.2f}", "inline": True},
        ],
        "footer": {"text": f"QuickCap Live Signal • {_now_iso()}"}
    }
    if basis_pct is not None and basis_z is not None:
        embed["fields"].append({"name": "Basis %", "value": f"{basis_pct:.3f}", "inline": True})
        embed["fields"].append({"name": "Basis Z", "value": f"{basis_z:.3f}", "inline": True})

    await _post_json(WEBHOOK_LIVE, {"embeds": [embed]})

async def post_backfill_summary(venue: str, symbol: str, interval: str,
                                signals: int, executions: int, outcomes: int):
    """Send backfill completion summary → #sniper-backfill"""
    msg = f"✅ **Backfill** `{venue}:{symbol}:{interval}` → signals={signals} • executions={executions} • outcomes={outcomes}"
    await _post_json(WEBHOOK_BACKFILL, {"content": msg[:1990]})

async def post_performance_text(content: str):
    """Send performance reports → #sniper-performance"""
    await _post_json(WEBHOOK_PERFORMANCE, {"content": content[:1990]})

async def post_error(msg: str):
    """Send error logs → #sniper-errors"""
    await _post_json(WEBHOOK_ERRORS, {"content": f"⚠️ **Error:** {msg}"[:1990]})
