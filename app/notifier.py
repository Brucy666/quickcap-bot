# app/notifier.py
from __future__ import annotations

import os
import atexit
import asyncio
import aiohttp
from typing import Iterable, Optional


# --- Webhooks from environment ------------------------------------------------
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "").strip()


def _side_color(side: str) -> int:
    """Discord embed color by side."""
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F


def _fmt_trigs(trigs: Iterable[str]) -> str:
    """Nice compact bullet list for triggers."""
    t = [str(x).strip() for x in (trigs or []) if str(x).strip()]
    return " • ".join(t) if t else "—"


class DiscordNotifier:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        # Make sure we close the client on interpreter exit
        atexit.register(self._close_sync)

    # --- session management ----------------------------------------------------
    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8))
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _close_sync(self) -> None:
        """Best-effort close if event loop is gone (process exit)."""
        try:
            if self._session and not self._session.closed:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._session.close())
                else:
                    loop.run_until_complete(self._session.close())
        except Exception:
            pass

    # --- low-level POST --------------------------------------------------------
    async def _post(self, url: str, payload: dict) -> None:
        if not url:
            return
        try:
            async with self.session.post(url, json=payload) as r:
                if r.status >= 300:
                    txt = await r.text()
                    raise RuntimeError(f"Discord POST {r.status}: {txt}")
        except Exception as e:
            # keep this ultra-safe; never raise out
            print(f"[NOTIFY] {e}")

    # --- public APIs -----------------------------------------------------------
    async def post_signal_embed(  # <— name used by main.py
        self,
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
        basis_pct: Optional[float] = None,
        basis_z: Optional[float] = None,
    ) -> None:
        """Send a Discord embed for a live signal (spot or basis)."""
        if not WEBHOOK_LIVE:
            return

        fields = [
            {"name": "Side",     "value": side,            "inline": True},
            {"name": "Interval", "value": interval,        "inline": True},
            {"name": "Score",    "value": f"{score:0.3f}", "inline": True},
            {"name": "Price",    "value": f"{price:g}",    "inline": True},
            {"name": "VWAP",     "value": f"{vwap:g}",     "inline": True},
            {"name": "RSI",      "value": f"{rsi:0.2f}",   "inline": True},
            {"name": "Triggers", "value": _fmt_trigs(triggers), "inline": False},
        ]
        if basis_pct is not None:
            fields.append({"name": "Basis %", "value": f"{basis_pct:0.4f}", "inline": True})
        if basis_z is not None:
            fields.append({"name": "Basis Z", "value": f"{basis_z:0.2f}", "inline": True})

        embed = {
            "title": f"{exchange}:{symbol} • {side}",
            "color": _side_color(side),
            "fields": fields,
        }
        await self._post(WEBHOOK_LIVE, {"embeds": [embed]})

    # Backward-compatible alias (some older code may still call signal_embed)
    async def signal_embed(self, **kwargs) -> None:  # type: ignore[override]
        await self.post_signal_embed(**kwargs)

    async def backfill_summary(
        self,
        venue: str,
        symbol: str,
        interval: str,
        signals: int,
        executions: int,
        outcomes: int,
    ) -> None:
        if not WEBHOOK_BACKFILL:
            return
        msg = (
            "✅ **Backfill** "
            f"`{venue}:{symbol}:{interval}` → "
            f"signals={signals} • executions={executions} • outcomes={outcomes}"
        )
        await self._post(WEBHOOK_BACKFILL, {"content": msg[:1990]})

    async def performance(self, content: str) -> None:
        if WEBHOOK_PERFORMANCE:
            await self._post(WEBHOOK_PERFORMANCE, {"content": content[:1990]})

    async def error(self, msg: str) -> None:
        if WEBHOOK_ERRORS:
            await self._post(WEBHOOK_ERRORS, {"content": f"⚠️ **Error:** {msg}"[:1990]})


# Global singleton
NOTIFY = DiscordNotifier()
