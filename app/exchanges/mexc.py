import aiohttp
from .base import ExchangePublic

MEXC = "https://api.mexc.com"
MEXC_INTERVAL_MAP = {"1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "1h": "1h"}

class MEXCPublic(ExchangePublic):
    BASE = MEXC

    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        """Return [[ts, o, h, l, c, v]]; ts already in milliseconds from MEXC."""
        params = {"symbol": symbol, "interval": MEXC_INTERVAL_MAP.get(interval, "1m"), "limit": limit}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(self.BASE + "/api/v3/klines", params=params) as r:
                try:
                    resp = await r.json()
                except Exception:
                    return []
                data = resp if isinstance(resp, list) else []
                out: list[list] = []
                for row in data:
                    try:
                        ts = int(row[0]); o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4]); v = float(row[5])
                        out.append([ts, o, h, l, c, v])
                    except Exception:
                        continue
                return out

    @staticmethod
    async def top_symbols(top_n: int = 20, min_vol_usdt: float = 0.0) -> list[str]:
        """Top USDT pairs by 24h quote volume and abs % change."""
        url = MEXC + "/api/v3/ticker/24hr"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(url) as r:
                try:
                    resp = await r.json()
                except Exception:
                    return []
                data = resp if isinstance(resp, list) else []
                rows = []
                for d in data:
                    try:
                        sym = d.get("symbol", "")
                        if not sym.endswith("USDT"):
                            continue
                        vol_usdt = float(d.get("quoteVolume", 0.0))
                        chg = abs(float(d.get("priceChangePercent", 0.0)))
                        rows.append((sym, vol_usdt, chg))
                    except Exception:
                        continue
                rows.sort(key=lambda x: (x[1], x[2]), reverse=True)
                out = []
                for sym, vol, _ in rows:
                    if vol >= min_vol_usdt:
                        out.append(sym)
                    if len(out) >= top_n:
                        break
                return out
