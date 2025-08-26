# app/notifier.py
from __future__ import annotations
import os
import aiohttp
import asyncio
from datetime import datetime, timezone
from typing import Iterable, Optional

# ---- Webhook URLs from ENV or defaults ----
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "").strip()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _fmt_triggers(trigs: Iterable[str]) -> str:
    if not trigs: return "—"
    return " • ".join([str(x) for x in trigs if str(x).strip()])

def _side_color(side: str) -> int:
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F


# ----------------- SESSION HANDLER -----------------
class DiscordNotifier:
    """Singleton async session wrapper for Discord webhooks"""
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def post(self, url: str, payload: dict):
        if not url:
            return
        try:
            session = await self._ensure_session()
            async with session.post(url, json=payload) as r:
                if r.status >= 300:
                    txt = await r.text()
                    print(f"[DISCORD] Failed {r.status}: {txt}")
        except Exception as e:
            print(f"[DISCORD] Error posting: {e}")


NOTIFY = DiscordNotifier()  # global singleton


# ----------------- PUBLIC API -----------------

async def post_signal_embed(
    exchange: str,
    symbol: str,
    interval: str,
    side: str,
    price: float,
    vwap: float,
    rsi: float,
    score: float,
    triggers: list[str],
    basis_pct: float | None = None,
    basis_z: float | None = None,
):
    """Send live trade signal to #sniper-live"""
    embed = {
        "title": f"📊 {exchange}:{symbol} {side}",
        "color": _side_color(side),
        "fields": [
            {"name": "Exchange", "value": exchange, "inline": True},
            {"name": "Interval", "value": interval, "inline": True},
            {"name": "Price", "value": f"{price:.4f}", "inline": True},
            {"name": "VWAP", "value": f"{vwap:.4f}", "inline": True},
            {"name": "RSI", "value": f"{rsi:.2f}", "inline": True},
            {"name": "Score", "value": f"{score:.2f}", "inline": True},
            {"name": "Triggers", "value": _fmt_triggers(triggers), "inline": False},
        ],
        "timestamp": _now_iso(),
    }
    if basis_pct is not None:
        embed["fields"].append({"name": "Basis %", "value": f"{basis_pct:.3f}", "inline": True})
    if basis_z is not None:
        embed["fields"].append({"name": "Basis Z", "value": f"{basis_z:.2f}", "inline": True})

    await NOTIFY.post(WEBHOOK_LIVE, {"embeds": [embed]})


async def post_backfill_summary(venue: str, symbol: str, interval: str, signals: int, executions: int, outcomes: int):
    msg = f"✅ **Backfill** `{venue}:{symbol}:{interval}` → signals={signals} • executions={executions} • outcomes={outcomes}"
    await NOTIFY.post(WEBHOOK_BACKFILL, {"content": msg})


async def post_performance_text(content: str):
    await NOTIFY.post(WEBHOOK_PERFORMANCE, {"content": f"📈 **Performance Report**\n```{content[:1900]}```"})


async def post_error(msg: str):
    await NOTIFY.post(WEBHOOK_ERRORS, {"content": f"⚠️ **Error:** {msg[:1900]}"})
