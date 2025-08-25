# app/notifier.py
from __future__ import annotations
import os, aiohttp
from datetime import datetime, timezone
from typing import Iterable, Optional

ALERTS_WEBHOOK = os.environ.get("DISCORD_ALERTS_WEBHOOK", "").strip()
PERF_WEBHOOK   = os.environ.get("DISCORD_PERF_WEBHOOK", "").strip()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _post_json(url: str, payload: dict):
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload) as r:
            if r.status >= 300:
                raise RuntimeError(f"Discord post failed {r.status}: {await r.text()}")

def _fmt_triggers(trigs: Iterable[str]) -> str:
    t = [str(x) for x in trigs if str(x).strip()]
    return " • ".join(t) if t else "—"

def _side_color(side: str) -> int:
    return 0x13A10E if (side or "").upper() == "LONG" else 0xC50F1F

async def post_signal_embed(
    webhook_url: Optional[str] = None,
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
    """Live signal embed → #sniper-confluence-alerts"""
    url = (webhook_url or ALERTS_WEBHOOK).strip()
    if not url:
        print("[NO-WEBHOOK] post_signal_embed"); return
    embed = {
        "title": f"{symbol} • {side}",
        "description": _fmt_triggers(triggers),
        "color": _side_color(side),
        "timestamp": _now_iso(),
        "fields": [
            {"name":"Exchange","value":exchange,"inline":True},
            {"name":"Interval","value":interval,"inline":True},
            {"name":"Score","value":f"{score:0.3f}","inline":True},
            {"name":"Price","value":f"{price}","inline":True},
            {"name":"VWAP","value":f"{vwap}","inline":True},
            {"name":"RSI","value":f"{rsi:0.2f}","inline":True},
        ],
        "footer":{"text":"QuickCap • Live Signal"},
    }
    await _post_json(url, {"embeds":[embed]})

async def post_text_to_performance(content: str, webhook_url: Optional[str] = None):
    """Plain text → #signal-performance"""
    url = (webhook_url or PERF_WEBHOOK).strip()
    if not url:
        print("[NO-WEBHOOK] post_text_to_performance"); print(content); return
    await _post_json(url, {"content": content[:1990]})

async def post_backfill_summary(venue: str, symbol: str, interval: str, signals: int, executions: int, outcomes: int):
    line = f"✅ **Backfill** `{venue}:{symbol}:{interval}` → signals={signals} • executions={executions} • outcomes={outcomes}"
    await post_text_to_performance(line)
