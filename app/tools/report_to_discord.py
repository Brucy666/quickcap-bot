# app/tools/report_to_discord.py
import aiohttp
import asyncio
import json
from app.logger import get_logger

log = get_logger("report_to_discord")

# Map logical channels to Discord webhook URLs
WEBHOOKS = {
    "live": "https://discord.com/api/webhooks/1409631433865302217/BKWwGFqa7vK-l3V1sY5e5aGFq8x0LayqGDYrM6-0OE6xeQC8rFSqMfrAzUFxZeAA1bCJ",
    "backfill": "https://discord.com/api/webhooks/1409631717311909919/wpoF7-XrwJ10eqpo0uo0apJha_nrHgL4iHvi2EWuLy3PFxle71V_sXBDN0tSKsfHaDQA",
    "errors": "https://discord.com/api/webhooks/1409632131206086708/yTe-T1NcT72UFcY7i33ar-ZITVnrE6DbmPvWla8aek519TZhy--W3mERbH_Vd7z3XJn5",
    "performance": "https://discord.com/api/webhooks/1409633072097529866/zT3fA34Exzbtn3oLn1-jFu-JY7IO_8cWoBrOxRvcTIZS5nZwUL-V22s5BQntyEsiKvag",
}

async def _post_message(webhook_url: str, content: str = None, embed: dict = None):
    """Low-level function to post message or embed to Discord."""
    async with aiohttp.ClientSession() as session:
        payload = {}
        if content:
            payload["content"] = content
        if embed:
            payload["embeds"] = [embed]
        try:
            async with session.post(webhook_url, json=payload) as resp:
                if resp.status != 204:
                    txt = await resp.text()
                    log.error(f"Discord webhook failed {resp.status}: {txt}")
        except Exception as e:
            log.exception(f"Discord webhook error: {e}")

# ---------- PUBLIC HELPERS ----------

async def post_signal_embed(channel: str, exchange: str, symbol: str, side: str,
                            price: float, score: float, triggers: list, interval: str = "1m"):
    """Send a trading signal embed to a Discord channel."""
    if channel not in WEBHOOKS:
        log.error(f"Unknown channel {channel}")
        return

    embed = {
        "title": f"📈 Signal on {exchange}:{symbol}",
        "color": 0x00ff00 if side == "LONG" else 0xff0000,
        "fields": [
            {"name": "Side", "value": side, "inline": True},
            {"name": "Price", "value": f"{price:.4f}", "inline": True},
            {"name": "Score", "value": f"{score:.2f}", "inline": True},
            {"name": "Triggers", "value": ", ".join(triggers), "inline": False},
            {"name": "Interval", "value": interval, "inline": True},
        ],
    }
    await _post_message(WEBHOOKS[channel], embed=embed)

async def post_backfill_summary(report_text: str):
    """Send a backfill summary report to Discord."""
    await _post_message(WEBHOOKS["backfill"], content=f"**Backfill Report**\n```{report_text}```")

async def post_error(msg: str):
    """Send error messages to Discord."""
    await _post_message(WEBHOOKS["errors"], content=f"❌ Error:\n```{msg}```")

async def post_performance_report(report_text: str):
    """Send performance metrics to Discord."""
    await _post_message(WEBHOOKS["performance"], content=f"**Sniper Performance Report**\n```{report_text}```")
