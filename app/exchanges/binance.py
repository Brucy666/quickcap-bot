import aiohttp
from .base import ExchangePublic

BINANCE_SPOT = "https://api.binance.com"
BINANCE_PERP = "https://fapi.binance.com"
BIN_INTERVAL = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","1h":"1h"}

def _safe_rows(resp):
    # spot/perp klines return a list; errors return dict with 'code'
    return resp if isinstance(resp, list) else []

class BinanceSpotPublic(ExchangePublic):
    BASE = BINANCE_SPOT
    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        params = {"symbol": symbol, "interval": BIN_INTERVAL.get(interval,"1m"), "limit": limit}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(self.BASE + "/api/v3/klines", params=params) as r:
                data = _safe_rows(await r.json())
                out = []
                for row in data:
                    try:
                        ts = int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
                        out.append([ts,o,h,l,c,v])
                    except Exception:
                        continue
                return out

    @staticmethod
    async def top_symbols(top_n: int = 20, min_vol_usdt: float = 0.0) -> list[str]:
        url = BINANCE_SPOT + "/api/v3/ticker/24hr"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(url) as r:
                data = await r.json()
                rows = []
                for d in (data if isinstance(data, list) else []):
                    sym = d.get("symbol","")
                    if not sym.endswith("USDT"): continue
                    vol_usdt = float(d.get("quoteVolume", 0.0))
                    chg = abs(float(d.get("priceChangePercent", 0.0)))
                    rows.append((sym, vol_usdt, chg))
                rows.sort(key=lambda x: (x[1], x[2]), reverse=True)
                out=[]
                for sym, vol, _ in rows:
                    if vol >= min_vol_usdt: out.append(sym)
                    if len(out) >= top_n: break
                return out

class BinancePerpPublic(ExchangePublic):
    BASE = BINANCE_PERP
    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        params = {"symbol": symbol, "interval": BIN_INTERVAL.get(interval,"1m"), "limit": limit}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(self.BASE + "/fapi/v1/klines", params=params) as r:
                data = _safe_rows(await r.json())
                out=[]
                for row in data:
                    try:
                        ts=int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
                        out.append([ts,o,h,l,c,v])
                    except Exception:
                        continue
                return out

    @staticmethod
    async def top_symbols(top_n: int = 20, min_vol_usdt: float = 0.0) -> list[str]:
        url = BINANCE_PERP + "/fapi/v1/ticker/24hr"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(url) as r:
                data = await r.json()
                rows=[]
                for d in (data if isinstance(data, list) else []):
                    sym = d.get("symbol","")
                    if not sym.endswith("USDT"): continue
                    vol_usdt = float(d.get("quoteVolume", 0.0))
                    chg = abs(float(d.get("priceChangePercent", 0.0)))
                    rows.append((sym, vol_usdt, chg))
                rows.sort(key=lambda x:(x[1], x[2]), reverse=True)
                out=[]
                for sym, vol, _ in rows:
                    if vol >= min_vol_usdt: out.append(sym)
                    if len(out) >= top_n: break
                return out
