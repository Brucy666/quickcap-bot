# app/storage/supabase.py
import aiohttp, asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from urllib.parse import urlparse

JSON = Dict[str, Any]

def _normalize_base(url: str) -> str:
    """
    Acceptable inputs:
      https://xxx.supabase.co
      https://xxx.supabase.co/
      https://xxx.supabase.co/rest/v1
      https://xxx.supabase.co/%2Frest%2Fv1
    Returns canonical base: https://xxx.supabase.co
    """
    u = (url or "").strip().rstrip("/")
    u = u.replace("%2F", "/").replace("%2f", "/")
    if u.endswith("/rest/v1"):
        u = u[:-len("/rest/v1")]
    parsed = urlparse(u)
    if not (parsed.scheme and parsed.netloc):
        raise ValueError(f"Invalid SUPABASE_URL: {url!r}")
    return u

class Supa:
    def __init__(self, url: str, key: str):
        self.base = _normalize_base(url)
        self.key = key

    # ---------- low level ----------
    def _headers(self) -> dict:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    async def _post(self, table: str, rows: List[JSON], params: Optional[dict] = None) -> Optional[List[JSON]]:
        if not rows:
            return None
        endpoint = f"{self.base}/rest/v1/{table}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as s:
                async with s.post(endpoint, headers=self._headers(), json=rows, params=params or {"select": "*"}) as r:
                    # Swallow non-2xx without crashing the bot
                    if r.status // 100 != 2:
                        _ = await r.text()
                        return None
                    try:
                        return await r.json()
                    except Exception:
                        return None
        except Exception:
            return None

    async def select(self, table: str, params: dict) -> List[JSON]:
        """GET /rest/v1/<table>?<params>; returns [] on error."""
        endpoint = f"{self.base}/rest/v1/{table}"
        q = dict(params or {})
        q.setdefault("select", "*")
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as s:
                async with s.get(endpoint, headers=self._headers(), params=q) as r:
                    if r.status // 100 != 2:
                        _ = await r.text()
                        return []
                    try:
                        return await r.json()
                    except Exception:
                        return []
        except Exception:
            return []

    async def insert(self, table: str, rows: List[JSON]) -> Optional[List[JSON]]:
        """Simple INSERT; returns created rows or None on error."""
        return await self._post(table, rows, params={"select": "*"})

    async def upsert(self, table: str, rows: List[JSON], on_conflict: str) -> Optional[List[JSON]]:
        """UPSERT using `on_conflict` columns, merge-duplicates preference."""
        if not rows:
            return None
        endpoint = f"{self.base}/rest/v1/{table}"
        params = {"on_conflict": on_conflict, "select": "*"}
        headers = self._headers() | {"Prefer": "resolution=merge-duplicates,return=representation"}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as s:
                async with s.post(endpoint, headers=headers, params=params, json=rows) as r:
                    if r.status // 100 != 2:
                        _ = await r.text()
                        return None
                    try:
                        return await r.json()
                    except Exception:
                        return None
        except Exception:
            return None

    # ---------- fire-and-forget wrappers ----------
    def log_signal_bg(self, **kwargs) -> None:
        asyncio.create_task(self.log_signal(**kwargs))

    def log_execution_bg(self, **kwargs) -> None:
        asyncio.create_task(self.log_execution(**kwargs))

    # ---------- domain writers ----------
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
    ) -> None:
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
        await self.insert("signals", [row])

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
    ) -> None:
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
        await self.insert("executions", [row])
