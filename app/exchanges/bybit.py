import aiohttp
from .base import ExchangePublic

BYBIT = "https://api.bybit.com"
MAP = {"1m":"1","3m":"3","5m":"5","15m":"15","1h":"60"}

class BybitSpotPublic(ExchangePublic):
    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        params = {"category":"spot","symbol":symbol,"interval":MAP.get(interval,"1"),"limit":min(limit,1000)}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(BYBIT + "/v5/market/kline", params=params) as r:
                data = (await r.json())["result"]["list"]
                out=[]; 
                for row in reversed(data):
                    ts=int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
                    out.append([ts,o,h,l,c,v])
                return out

    @staticmethod
    async def top_symbols(top_n: int=20, min_vol_usdt: float=0.0) -> list[str]:
        params={"category":"spot"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(BYBIT + "/v5/market/tickers", params=params) as r:
                data=(await r.json())["result"]["list"]
                rows=[]
                for d in data:
                    sym = d.get("symbol","")
                    if not sym.endswith("USDT"): continue
                    vol_usdt = float(d.get("turnover24h",0.0))
                    chg = float(d.get("price24hPcnt",0.0))*100.0
                    rows.append((sym, vol_usdt, abs(chg)))
                rows.sort(key=lambda x:(x[1], x[2]), reverse=True)
                out=[]
                for sym, vol, _ in rows:
                    if vol>=min_vol_usdt: out.append(sym)
                    if len(out)>=top_n: break
                return out

class BybitPerpPublic(ExchangePublic):
    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        params={"category":"linear","symbol":symbol,"interval":MAP.get(interval,"1"),"limit":min(limit,1000)}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(BYBIT + "/v5/market/kline", params=params) as r:
                data=(await r.json())["result"]["list"]
                out=[]; 
                for row in reversed(data):
                    ts=int(row[0]); o=float(row[1]); h=float(row[2]); l=float(row[3]); c=float(row[4]); v=float(row[5])
                    out.append([ts,o,h,l,c,v])
                return out

    @staticmethod
    async def top_symbols(top_n: int=20, min_vol_usdt: float=0.0) -> list[str]:
        params={"category":"linear"}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(BYBIT + "/v5/market/tickers", params=params) as r:
                data=(await r.json())["result"]["list"]
                rows=[]
                for d in data:
                    sym = d.get("symbol","")
                    if not sym.endswith("USDT"): continue
                    vol_usdt = float(d.get("turnover24h",0.0))
                    chg = float(d.get("price24hPcnt",0.0))*100.0
                    rows.append((sym, vol_usdt, abs(chg)))
                rows.sort(key=lambda x:(x[1], x[2]), reverse=True)
                out=[]
                for sym, vol, _ in rows:
                    if vol>=min_vol_usdt: out.append(sym)
                    if len(out)>=top_n: break
                return out
