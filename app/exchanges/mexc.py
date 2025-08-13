import aiohttp
from .base import ExchangePublic

MEXC_INTERVAL_MAP = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","1h":"1h"
}

class MEXCPublic(ExchangePublic):
    BASE = "https://api.mexc.com"

    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        pair = symbol  # MEXC uses e.g. BTCUSDT
        params = {"symbol": pair, "interval": MEXC_INTERVAL_MAP.get(interval,"1m"), "limit": limit}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(self.BASE + "/api/v3/klines", params=params) as r:
                data = await r.json()
                # Binance-style rows: [openTime, open, high, low, close, volume, ...]
                res = []
                for row in data:
                    ts = int(row[0])
                    o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4]); v = float(row[5])
                    res.append([ts,o,h,l,c,v])
                return res
