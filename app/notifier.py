# app/notifier.py
from __future__ import annotations

import os
from typing import Iterable, Optional

import aiohttp

# ---- Environment-configured webhooks ----
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "").strip()


def _side_color(side: str) -> int:
    """Green for LONG, red for SHORT."""
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F


def _fmt_trigs(trigs: Iterable[str]) -> str:
    """Nicely format trigger list for Discord."""
    t = [str(x).strip() for x in (trigs or []) if str(x).strip()]
    return " • ".join(t) if t else "—"


class DiscordNotifier:
    """Lightweight Discord embed + message poster with lazy aiohttp session."""

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8))
        return self._session

    async def _post(self, url: str, payload: dict) -> None:
        if not url:
            print("[NOTIFY] Skipping post: webhook URL empty")
            return
        try:
            async with self.session.post(url, json=payload) as r:
                if r.status >= 300:
                    txt = await r.text()
                    raise RuntimeError(f"Discord POST {r.status}: {txt}")
        except Exception as e:
            print(f"[NOTIFY] {e}")

    # ------------- Public APIs -------------

    async def signal_embed(
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
    ):
        """Send a live signal embed (works for spot & basis; basis fields optional)."""
        fields = [
            {"name": "Side",     "value": side,             "inline": True},
            {"name": "Interval", "value": interval,         "inline": True},
            {"name": "Score",    "value": f"{score:.3f}",   "inline": True},
            {"name": "Price",    "value": f"{price:g}",     "inline": True},
            {"name": "VWAP",     "value": f"{vwap:g}",      "inline": True},
            {"name": "RSI",      "value": f"{rsi:.2f}",     "inline": True},
            {"name": "Triggers", "value": _fmt_trigs(triggers), "inline": False},
        ]
        if basis_pct is not None:
            fields.append({"name": "Basis%", "value": f"{basis_pct:.4f}", "inline": True})
        if basis_z is not None:
            fields.append({"name": "Basis Z", "value": f"{basis_z:.2f}", "inline": True})

        embed = {
            "title": f"{exchange}:{symbol} • {side}",
            "color": _side_color(side),
            "fields": fields,
        }
        await self._post(WEBHOOK_LIVE, {"embeds": [embed]})

    async def backfill_summary(self, venue: str, symbol: str, interval: str,
                               signals: int, executions: int, outcomes: int):
        """One-line summary after a backfill batch for a symbol."""
        msg = (
            f"✅ **Backfill** `{venue}:{symbol}:{interval}` "
            f"→ signals={signals} • executions={executions} • outcomes={outcomes}"
        )
        await self._post(WEBHOOK_BACKFILL, {"content": msg[:1990]})

    async def performance(self, content: str):
        """Free-form performance text message."""
        await self._post(WEBHOOK_PERFORMANCE, {"content": content[:1990]})

    async def error(self, msg: str):
        """Post an error line to the errors channel."""
        await self._post(WEBHOOK_ERRORS, {"content": f"⚠️ **Error:** {msg}"[:1990]})


# Global singleton used across the app
NOTIFY = DiscordNotifier()
