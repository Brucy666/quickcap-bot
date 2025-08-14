import aiohttp
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

JSON = Dict[str, Any]

class Supa:
    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.key = key

    def _headers(self) -> dict:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    async def insert(self, table: str, rows: List[JSON]) -> Optional[List[JSON]]:
        if not rows:
            return None
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.post(f"{self.url}/rest/v1/{table}", headers=self._headers(), json=rows, params={"select":"*"}) as r:
                if r.status // 100 != 2:
                    return None
                try:
                    return await r.json()
                except Exception:
                    return None

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
        await self.insert("executions", [row])
