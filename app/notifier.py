import aiohttp
from datetime import datetime, timezone
from .logger import get_logger
log = get_logger("notifier")

# Discord colors
GREEN = 0x00C853  # long
RED   = 0xD50000  # short
BLUE  = 0x2962FF  # neutral/info

async def post_discord(webhook: str | None, content: str):
    """Plain text fallback."""
    if not webhook:
        log.info(f"[NO-WEBHOOK] {content}")
        return
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
        await s.post(webhook, json={"content": content})

async def post_signal_embed(
    webhook: str | None,
    *,
    exchange: str,
    symbol: str,
    interval: str,
    side: str,          # "LONG" or "SHORT"
    price: float,
    vwap: float,
    rsi: float,
    score: float,
    triggers: list[str]
):
    """Styled embed for trade signals."""
    color = GREEN if side.upper() == "LONG" else RED
    vwap_delta = (price - vwap) / (vwap + 1e-12) * 100.0
    title = f"{exchange.upper()} · {symbol} · {interval} · {side.upper()}"
    desc = " / ".join(triggers) if triggers else "Signal"

    embed = {
        "title": title,
        "description": desc,
        "color": color,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": "Price", "value": f"`{price:.4f}`", "inline": True},
            {"name": "VWAP", "value": f"`{vwap:.4f}`", "inline": True},
            {"name": "VWAPΔ", "value": f"`{vwap_delta:+.2f}%`", "inline": True},
            {"name": "RSI", "value": f"`{rsi:.1f}`", "inline": True},
            {"name": "Score", "value": f"**{score:.2f}**", "inline": True},
        ],
        "footer": {"text": "QuickCap · VWAP • RSI Div • Momentum"},
    }

    if not webhook:
        log.info(f"[NO-WEBHOOK] {title} :: {desc}")
        return

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
        await s.post(webhook, json={"embeds": [embed]})
