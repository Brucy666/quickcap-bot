import aiohttp
from .base import ExchangePublic

KUCOIN = "https://api.kucoin.com"
KU_INTERVAL_MAP = {"1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min", "1h": "1hour"}

class KuCoinPublic(ExchangePublic):
    BASE = KUCOIN

    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        """Return [[ts, o, h, l, c, v]]; ts in milliseconds."""
        pair = symbol.replace("USDT", "-USDT")
        params = {"type": KU_INTERVAL_MAP.get(interval, "1min"), "symbol": pair}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(self.BASE + "/api/v1/market/candles", params=params) as r:
                try:
                    resp = await r.json()
                except Exception:
                    return []
                data = resp.get("data") if isinstance(resp, dict) else None
                if not isinstance(data, list):
                    return []
                out: list[list] = []
                for row in reversed(data[-limit:]):  # newest last
                    try:
                        ts = int(row[0]) * 1000  # KuCoin ts in seconds
                        o = float(row[1]); c = float(row[2]); h = float(row[3]); l = float(row[4]); v = float(row[5])
                        out.append([ts, o, h, l, c, v])
                    except Exception:
                        continue
                return out

    @staticmethod
    async def top_symbols(top_n: int = 20, min_vol_usdt: float = 0.0) -> list[str]:
        """Top USDT pairs by 24h quote volume and abs % change."""
        url = KUCOIN + "/api/v1/market/stats"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(url) as r:
                try:
                    resp = await r.json()
                except Exception:
                    return []
                data = resp.get("data") if isinstance(resp, dict) else None
                if not isinstance(data, list):
                    return []
                rows = []
                for d in data:
                    try:
                        sym = d.get("symbol", "")
                        if not sym.endswith("-USDT"):
                            continue
                        vol_usdt = float(d.get("volValue", 0.0))
                        chg = abs(float(d.get("changeRate", 0.0)))  # 0.0123 = 1.23%
                        rows.append((sym.replace("-USDT", "USDT"), vol_usdt, chg))
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
