import aiohttp, asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from urllib.parse import urlparse

JSON = Dict[str, Any]

def _normalize_base(url: str) -> str:
    """
    Accepts:
      - https://xxx.supabase.co
      - https://xxx.supabase.co/
      - https://xxx.supabase.co/rest/v1
      - https://xxx.supabase.co/%2Frest%2Fv1
    Returns canonical: https://xxx.supabase.co
    """
    u = url.strip().rstrip("/")
    u = u.replace("%2F", "/").replace("%2f", "/")  # de-encode slashes if user pasted encoded
    if u.endswith("/rest/v1"):
        u = u[:-len("/rest/v1")]
    # sanity: must have scheme + netloc
    parsed = urlparse(u)
    if not (parsed.scheme and parsed.netloc):
        raise ValueError(f"Invalid SUPABASE_URL: {url!r}")
    return u

class Supa:
    def __init__(self, url: str, key: str):
        self.base = _normalize_base(url)
        self.key = key

    def _headers(self) -> dict:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    async def _post(self, table: str, rows: List[JSON]) -> Optional[List[JSON]]:
        if not rows:
            return None
        endpoint = f"{self.base}/rest/v1/{table}"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.post(endpoint, headers=self._headers(), json=rows, params={"select":"*"}) as r:
                if r.status // 100 != 2:
                    # swallow but return None to keep bot healthy
                    try:
                        _ = await r.text()
                    except Exception:
                        pass
                    return None
                try:
                    return await r.json()
                except Exception:
                    return None

    # ---- public, fire-and-forget wrappers ----
    def log_signal_bg(self, **kwargs) -> None:
        asyncio.create_task(self.log_signal(**kwargs))

    def log_execution_bg(self, **kwargs) -> None:
        asyncio.create_task(self.log_execution(**kwargs))

    async def log_signal(
        self,
        *,
        signal_type: str,  # "spot" | "basis"
        venue: str,
        symbol: str,
        interval: str,
        side: str,
        price: float,
        vwap: float,
        rsi: float,
        score: float,
        triggers: List[str],
        basis_pct: Optional[float] = None,
        basis_z: Optional[float] = None,
    ):
        row: JSON = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "signal_type": signal_type,
            "venue": venue,
            "symbol": symbol,
            "interval": interval,
            "side": side,
            "price": price,
            "vwap": vwap,
            "rsi": rsi,
            "score": score,
            "triggers": triggers,
            "basis_pct": basis_pct,
            "basis_z": basis_z,
        }
        await self._post("signals", [row])

    async def log_execution(
        self,
        *,
        venue: str,
        symbol: str,
        side: str,
        price: float,
        score: float,
        reason: str,
        is_paper: bool = True,
    ):
        row: JSON = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "price": price,
            "score": score,
            "reason": reason,
            "is_paper": is_paper,
        }
        await self._post("executions", [row])
