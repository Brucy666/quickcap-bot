from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import aiohttp

# ---------------------- Webhook Config ----------------------

WEBHOOK_LIVE = os.getenv(
    "DISCORD_WEBHOOK_LIVE",
    "https://discord.com/api/webhooks/1409631433865302217/BKWwGFqa7vK-l3V1sY5e5aGFq8x0LayqGDYrM6-0OE6xeQC8rFSqMfrAzUFxZeAA1bCJ",
).strip()

WEBHOOK_BACKFILL = os.getenv(
    "DISCORD_WEBHOOK_BACKFILL",
    "https://discord.com/api/webhooks/1409631717311909919/wpoF7-XrwJ10eqpo0uo0apJha_nrHgL4iHvi2EWuLy3PFxle71V_sXBDN0tSKsfHaDQA",
).strip()

WEBHOOK_ERRORS = os.getenv(
    "DISCORD_WEBHOOK_ERRORS",
    "https://discord.com/api/webhooks/1409632131206086708/yTe-T1NcT72UFcY7i33ar-ZITVnrE6DbmPvWla8aek519TZhy--W3mERbH_Vd7z3XJn5",
).strip()

WEBHOOK_PERF = os.getenv(
    "DISCORD_WEBHOOK_PERFORMANCE",
    "https://discord.com/api/webhooks/1409633072097529866/zT3fA34Exzbtn3oLn1-jFu-JY7IO_8cWoBrOxRvcTIZS5nZwUL-V22s5BQntyEsiKvag",
).strip()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fmt_triggers(trigs: Optional[Iterable[str]]) -> str:
    if not trigs:
        return "—"
    t = [str(x).strip() for x in trigs if str(x).strip()]
    return " • ".join(t) if t else "—"


def _side_color(side: str) -> int:
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F


@dataclass
class NotifierConfig:
    live: str = WEBHOOK_LIVE
    backfill: str = WEBHOOK_BACKFILL
    errors: str = WEBHOOK_ERRORS
    performance: str = WEBHOOK_PERF


class DiscordNotifier:
    """
    A resilient, single-session Discord webhook notifier.
    """

    def __init__(self, cfg: NotifierConfig | None = None):
        self.cfg = cfg or NotifierConfig()
        self._session: Optional[aiohttp.ClientSession] = None
        # simple lock so we don't race session creation
        self._lock = asyncio.Lock()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None or self._session.closed:
                # A quiet, connection-pooled session
                timeout = aiohttp.ClientTimeout(total=15)
                self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # --------------- Low-level ---------------

    async def _post_json(self, url: str, payload: dict):
        if not url:
            return
        session = await self._ensure_session()
        try:
            async with session.post(url, json=payload) as r:
                if r.status >= 300:
                    text = await r.text()
                    raise RuntimeError(f"Discord post failed {r.status}: {text[:500]}")
        except aiohttp.ClientConnectionError:
            # recreate session once and retry once
            await self.close()
            session = await self._ensure_session()
            async with session.post(url, json=payload) as r2:
                if r2.status >= 300:
                    text2 = await r2.text()
                    raise RuntimeError(f"Discord post failed {r2.status}: {text2[:500]}")

    # --------------- Public API ---------------

    async def post_signal_embed(
        self,
        exchange: str,
        symbol: str,
        interval: str,
        side: str,
        price: float,
        vwap: float,
        rsi: float,
        score: float,
        triggers: Iterable[str] | None = None,
        *,
        # optional basis fields
        basis_pct: float | None = None,
        basis_z: float | None = None,
    ):
        """
        Send a live trade/signal embed to #sniper-live.
        """
        fields = [
            {"name": "Side", "value": side, "inline": True},
            {"name": "Interval", "value": str(interval), "inline": True},
            {"name": "Score", "value": f"{score:.3f}", "inline": True},
            {"name": "Price", "value": f"{price:.6f}", "inline": True},
            {"name": "VWAP", "value": f"{vwap:.6f}", "inline": True},
            {"name": "RSI", "value": f"{rsi:.2f}", "inline": True},
            {"name": "Triggers", "value": _fmt_triggers(triggers), "inline": False},
        ]

        # Add basis context if provided
        if basis_pct is not None or basis_z is not None:
            fields.append(
                {
                    "name": "Basis",
                    "value": f"pct={basis_pct:.4f} • z={basis_z:.3f}"
                    if (basis_pct is not None and basis_z is not None)
                    else (
                        f"pct={basis_pct:.4f}" if basis_pct is not None else f"z={basis_z:.3f}"
                    ),
                    "inline": True,
                }
            )

        embed = {
            "title": f"{symbol} • {side.upper()}",
            "description": f"`{exchange}`",
            "timestamp": _now_iso(),
            "color": _side_color(side),
            "fields": fields,
        }
        await self._post_json(self.cfg.live, {"embeds": [embed]})

    async def post_backfill_summary(
        self, venue: str, symbol: str, interval: str, signals: int, executions: int, outcomes: int
    ):
        msg = (
            f"✅ **Backfill** `{venue}:{symbol}:{interval}` → "
            f"signals={signals} • executions={executions} • outcomes={outcomes}"
        )
        await self._post_json(self.cfg.backfill, {"content": msg[:1990]})

    async def post_performance_text(self, content: str):
        await self._post_json(self.cfg.performance, {"content": content[:1990]})

    async def post_error(self, msg: str):
        await self._post_json(self.cfg.errors, {"content": f"⚠️ **Error:** {msg}"[:1990]})


# ----------------- Singleton + function shims -----------------

NOTIFY = DiscordNotifier()

# Backwards-compat function exports (old code imports these by name)
async def post_signal_embed(*args, **kwargs):
    return await NOTIFY.post_signal_embed(*args, **kwargs)


async def post_backfill_summary(*args, **kwargs):
    return await NOTIFY.post_backfill_summary(*args, **kwargs)


async def post_performance_text(*args, **kwargs):
    return await NOTIFY.post_performance_text(*args, **kwargs)


async def post_error(*args, **kwargs):
    return await NOTIFY.post_error(*args, **kwargs)
