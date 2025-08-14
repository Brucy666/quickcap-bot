import aiohttp
from .logger import get_logger
log = get_logger("notifier")

async def post_discord(webhook: str, content: str):
    if not webhook:
        log.info(f"[NO-WEBHOOK] {content}")
        return
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
        await s.post(webhook, json={"content": content})
