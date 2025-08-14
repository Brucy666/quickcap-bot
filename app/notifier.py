import aiohttp
from datetime import datetime, timezone
from .logger import get_logger
log = get_logger("notifier")

GREEN = 0x00C853
RED   = 0xD50000
BLUE  = 0x2962FF

async def post_signal_embed(
    webhook: str | None,
    *,
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
    color = GREEN if side.upper() == "LONG" else RED
    vwap_delta = (price - vwap) / (vwap + 1e-12) * 100.0
    title = f"{exchange.upper()} · {symbol} · {interval} · {side.upper()}"
    desc = " / ".join(triggers) if triggers else "Signal"

    fields = [
        {"name":"Price", "value": f"`{price:.4f}`", "inline": True},
        {"name":"VWAP",  "value": f"`{vwap:.4f}`",  "inline": True},
        {"name":"VWAPΔ", "value": f"`{vwap_delta:+.2f}%`", "inline": True},
        {"name":"RSI",   "value": f"`{rsi:.1f}`", "inline": True},
        {"name":"Score", "value": f"**{score:.2f}**", "inline": True},
    ]
    if basis_pct is not None and basis_z is not None:
        fields.extend([
            {"name":"Basis%", "value": f"`{basis_pct:+.3f}%`", "inline": True},
            {"name":"Basis Z", "value": f"`{basis_z:+.2f}`", "inline": True},
        ])

    embed = {
        "title": title, "description": desc, "color": color,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": fields,
        "footer": {"text": "QuickCap · VWAP • RSI • Momentum • Basis"},
    }

    if not webhook:
        log.info(f"[NO-WEBHOOK] {title} :: {desc}")
        return
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
        await s.post(webhook, json={"embeds": [embed]})
