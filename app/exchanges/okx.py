import aiohttp
from .base import ExchangePublic

OKX = "https://www.okx.com"
OKX_INTERVAL = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","1h":"1H"}

def okx_spot_symbol(symbol: str) -> str:
    return symbol.replace("USDT","-USDT")

def okx_perp_symbol(symbol: str) -> str:
    return symbol.replace("USDT","-USDT") + "-SWAP"

class OKXSpotPublic(ExchangePublic):
    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        inst = okx_spot_symbol(symbol)
        params = {"instId": inst, "bar": OKX_INTERVAL.get(interval,"1m"), "limit": str(limit)}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(OKX + "/api/v5/market/candles", params=params) as r:
                resp = await r.json()
                data = resp.get("data") if isinstance(resp, dict) else None
                if not isinstance(data, list): return []
                out=[]
                for row in reversed(data):
                    try:
                        ts=int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
                        out.append([ts,o,h,l,c,v])
                    except Exception:
                        continue
                return out

    @staticmethod
    async def top_symbols(top_n: int = 20, min_vol_usdt: float = 0.0) -> list[str]:
        params = {"instType":"SPOT"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(OKX + "/api/v5/market/tickers", params=params) as r:
                resp = await r.json()
                data = resp.get("data") if isinstance(resp, dict) else None
                rows=[]
                for d in (data or []):
                    inst = d.get("instId","")
                    if not inst.endswith("USDT"): continue
                    vol_usdt = float(d.get("volCcy24h", 0.0))
                    chg = abs(float(d.get("change24h", 0.0)))
                    sym = inst.replace("-USDT","USDT").replace("-","")
                    rows.append((sym, vol_usdt, chg))
                rows.sort(key=lambda x:(x[1], x[2]), reverse=True)
                out=[]
                for sym, vol, _ in rows:
                    if vol >= min_vol_usdt: out.append(sym)
                    if len(out) >= top_n: break
                return out

class OKXPerpPublic(ExchangePublic):
    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        inst = okx_perp_symbol(symbol)
        params = {"instId": inst, "bar": OKX_INTERVAL.get(interval,"1m"), "limit": str(limit)}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(OKX + "/api/v5/market/candles", params=params) as r:
                resp = await r.json()
                data = resp.get("data") if isinstance(resp, dict) else None
                if not isinstance(data, list): return []
                out=[]
                for row in reversed(data):
                    try:
                        ts=int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
                        out.append([ts,o,h,l,c,v])
                    except Exception:
                        continue
                return out

    @staticmethod
    async def top_symbols(top_n: int = 20, min_vol_usdt: float = 0.0) -> list[str]:
        params = {"instType":"SWAP"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(OKX + "/api/v5/market/tickers", params=params) as r:
                resp = await r.json()
                data = resp.get("data") if isinstance(resp, dict) else None
                rows=[]
                for d in (data or []):
                    inst = d.get("instId","")
                    if not inst.endswith("USDT-SWAP"): continue
                    vol_usdt = float(d.get("volCcy24h", 0.0))
                    chg = abs(float(d.get("change24h", 0.0)))
                    base = inst.split("-USDT")[0]
                    sym = f"{base}USDT"
                    rows.append((sym, vol_usdt, chg))
                rows.sort(key=lambda x:(x[1], x[2]), reverse=True)
                out=[]
                for sym, vol, _ in rows:
                    if vol >= min_vol_usdt: out.append(sym)
                    if len(out) >= top_n: break
                return out
