# app/alpha/allowlist.py
from __future__ import annotations
import os
import aiohttp

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

async def is_allowed(
    venue: str,
    symbol: str,
    signal_type: str,   # "spot" | "basis"
    horizon_m: int      # 15/30/60 etc.
) -> bool:
    """
    Ask Supabase whether this (venue,symbol,signal_type,horizon) is permitted right now,
    based on your performance/filters. If SUPABASE_* is not configured, allow.
    """
    if not (SUPABASE_URL and SUPABASE_KEY):
        # No remote filter configured -> allow
        return True

    url = f"{SUPABASE_URL}/rest/v1/rpc/trading_allowlist"
    params = {
        "venue": venue,
        "symbol": symbol,
        "signal_type": signal_type,
        "horizon_m": horizon_m,
        "enabled": True,
        "select": "allowed"
    }
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "return=minimal",
    }

    try:
        async with aiohttp.ClientSession() as ses:
            async with ses.get(url, params=params, headers=headers) as r:
                if r.status >= 300:
                    return True  # fail-open
                data = await r.json()
                # Expecting e.g. {"allowed": true} or a row list; handle both:
                if isinstance(data, dict) and "allowed" in data:
                    return bool(data["allowed"])
                if isinstance(data, list) and data:
                    row = data[0]
                    return bool(row.get("allowed", True))
                return True
    except Exception:
        # Network/schema issues -> fail-open (don’t block the bot)
        return True
