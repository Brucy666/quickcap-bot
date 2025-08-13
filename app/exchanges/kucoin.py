import aiohttp, asyncio
from .base import ExchangePublic

KU_INTERVAL_MAP = {
    "1m":"1min","3m":"3min","5m":"5min","15m":"15min","1h":"1hour"
}

class KuCoinPublic(ExchangePublic):
    BASE = "https://api.kucoin.com"

    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        # KuCoin symbols use "-" separator: BTC-USDT
        pair = symbol.replace("USDT","-USDT")
        res = []
        path = f"/api/v1/market/candles"
        # KuCoin returns most-recent-first; we'll reverse
        params = {"type": KU_INTERVAL_MAP.get(interval, "1min"), "symbol": pair}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(self.BASE + path, params=params) as r:
                data = (await r.json())["data"]
                # data rows: [time, open, close, high, low, volume, turnover] (time in string)
                for row in reversed(data[-limit:]):
                    ts = int(row[0]) * 1000
                    o = float(row[1]); c = float(row[2]); h = float(row[3]); l = float(row[4]); v = float(row[5])
                    res.append([ts,o,h,l,c,v])
        return res
