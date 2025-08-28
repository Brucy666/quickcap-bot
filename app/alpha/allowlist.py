import aiohttp
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

async def is_allowed(venue: str, symbol: str, signal_type: str, horizon_m: int) -> bool:
    if not (SUPABASE_URL and SUPABASE_KEY):
        return True  # fail-open for local/paper testing
    url = f"{SUPABASE_URL}/rest/v1/trading_allowlist"
    params = {
        "venue": f"eq.{venue}",
        "symbol": f"eq.{symbol}",
        "signal_type": f"eq.{signal_type}",
        "horizon_m": f"eq.{horizon_m}",
        "enabled": "eq.true",
        "select": "venue"
    }
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    async with aiohttp.ClientSession() as ses:
        async with ses.get(url, params=params, headers=headers) as r:
            if r.status != 200:
                return True
            data = await r.json()
            return len(data) > 0
