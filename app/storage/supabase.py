# app/storage/supabase.py
from __future__ import annotations
import aiohttp
import asyncio
from typing import Any, Dict

from app.logger import get_logger

log = get_logger("supabase")

class Supa:
    """
    Supabase client wrapper with a single aiohttp session.
    Provides safe async logging for signals + executions.
    """
    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.key = key
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"apikey": self.key, "Authorization": f"Bearer {self.key}"}
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _post_upsert(self, table: str, row: Dict[str, Any]):
        """Safe upsert to Supabase (awaited, logs errors)."""
        session = await self._ensure_session()
        url = f"{self.url}/rest/v1/{table}"
        try:
            async with session.post(
                url,
                json=row,
                headers={"Prefer": "resolution=merge-duplicates"}
            ) as resp:
                if resp.status >= 300:
                    txt = await resp.text()
                    log.error(f"Supabase POST {table} failed {resp.status}: {txt}")
        except Exception as e:
            log.error(f"Supabase {table} post error: {e}")

    # --------------------------------------------------------------------------
    # Public log methods (awaited by caller)
    # --------------------------------------------------------------------------
    async def log_signal(self, **row):
        await self._post_upsert("signals", row)

    async def log_execution(self, **row):
        await self._post_upsert("executions", row)

    async def bulk_insert(self, table: str, rows: list[dict], on_conflict: str = ""):
        if not rows:
            return
        session = await self._ensure_session()
        url = f"{self.url}/rest/v1/{table}"
        headers = {"Prefer": "resolution=merge-duplicates"}
        if on_conflict:
            headers["Prefer"] = f"resolution=merge-duplicates, on_conflict={on_conflict}"
        try:
            async with session.post(url, json=rows, headers=headers) as resp:
                if resp.status >= 300:
                    txt = await resp.text()
                    log.error(f"Supabase bulk insert {table} failed {resp.status}: {txt}")
        except Exception as e:
            log.error(f"Supabase bulk insert {table} error: {e}")
