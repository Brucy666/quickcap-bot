# app/notifier.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable, Optional, Dict, Any

import aiohttp

# ----------------- Webhook configuration -----------------
WEBHOOK_LIVE        = os.getenv("DISCORD_WEBHOOK_LIVE",        "").strip()
WEBHOOK_BACKFILL    = os.getenv("DISCORD_WEBHOOK_BACKFILL",    "").strip()
WEBHOOK_ERRORS      = os.getenv("DISCORD_WEBHOOK_ERRORS",      "").strip()
WEBHOOK_PERFORMANCE = os.getenv("DISCORD_WEBHOOK_PERFORMANCE", "").strip()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _fmt_triggers(trigs: Iterable[str] | None) -> str:
    if not trigs:
        return "—"
    t = [str(x).strip() for x in trigs if str(x).strip()]
    return " • ".join(t) if t else "—"

def _side_color(side: str) -> int:
    s = (side or "").upper()
    # green for LONG, red for SHORT, grey default
    return 0x13A10E if s == "LONG" else (0xC50F1F if s == "SHORT" else 0x7A7A7A)

class DiscordNotifier:
    """
    Single-session Discord notifier.
    All calls are awaited and errors are surfaced via non-200 logs (no background tasks).
    """
    def __init__(
        self,
        live: str = WEBHOOK_LIVE,
        backfill: str = WEBHOOK_BACKFILL,
        errors: str = WEBHOOK_ERRORS,
        performance: str = WEBHOOK_PERFORMANCE,
    ):
        self.webhooks = {
            "live": live,
            "backfill": backfill,
            "errors": errors,
            "performance": performance,
        }
        self._session: Optional[aiohttp.ClientSession] = None

    # ----------------- session lifecycle -----------------
    async def _ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ----------------- low-level poster ------------------
    async def _post_json(self, url: str, payload: Dict[str, Any]):
        if not url:
            return  # webhook not configured => silently skip
        sess = await self._ensure()
        try:
            async with sess.post(url, json=payload) as resp:
                if resp.status >= 300:
                    txt = await resp.text()
                    print(f"[DISCORD] POST failed {resp.status}: {txt}")
        except Exception as e:
            print(f"[DISCORD] POST error: {e}")

    # ----------------- public helpers --------------------
    async def post_error(self, msg: str):
        """Plain text to #sniper-errors."""
        content = f"⚠️ **Error** ({_now_iso()}):\n```{msg[:1900]}```"
        await self._post_json(self.webhooks["errors"], {"content": content})

    async def post_backfill_summary(self, venue: str, symbol: str, interval: str,
                                    signals: int, executions: int, outcomes: int):
        """Summary line to #sniper-backfill."""
        content = (f"✅ **Backfill** `{venue}:{symbol}:{interval}` "
                   f"→ signals={signals} • executions={executions} • outcomes={outcomes}")
        await self._post_json(self.webhooks["backfill"], {"content": content[:1990]})

    async def post_performance_text(self, text_block: str):
        """Preformatted performance table(s) to #sniper-performance."""
        content = f"**Sniper Performance**\n```{text_block[:1900]}```"
        await self._post_json(self.webhooks["performance"], {"content": content})

    async def post_signal_embed(
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
        triggers: Iterable[str] | None = None,
        # optional spot-perp extras
        basis_pct: Optional[float] = None,
        basis_z: Optional[float] = None,
        channel: str = "live",  # which webhook bucket to use
    ):
        """
        Rich embed to #sniper-live (default) or any webhook key in self.webhooks.
        """
        fields = [
            {"name": "Exchange", "value": f"{exchange}", "inline": True},
            {"name": "Interval", "value": str(interval), "inline": True},
            {"name": "Score", "value": f"{score:.3f}", "inline": True},

            {"name": "Price", "value": f"{price:.6f}", "inline": True},
            {"name": "VWAP",  "value": f"{vwap:.6f}",  "inline": True},
            {"name": "RSI",   "value": f"{rsi:.2f}",   "inline": True},

            {"name": "Triggers", "value": _fmt_triggers(triggers), "inline": False},
        ]

        # add basis fields if provided
        if basis_pct is not None or basis_z is not None:
            if basis_pct is not None:
                fields.append({"name": "Basis %", "value": f"{basis_pct:.4f}", "inline": True})
            if basis_z is not None:
                fields.append({"name": "Basis Z", "value": f"{basis_z:.3f}", "inline": True})

        embed = {
            "title": f"{symbol} • {side.upper()}",
            "description": f"**{exchange}** • `{interval}` • {_now_iso()}",
            "color": _side_color(side),
            "fields": fields,
            "footer": {"text": "QuickCap • Live Signal"},
        }

        await self._post_json(self.webhooks.get(channel, WEBHOOK_LIVE), {"embeds": [embed]})

# ----------------- simple module-level instance -----------------
# If you prefer: from app.notifier import NOTIFY and call methods on it.
NOTIFY = DiscordNotifier()
