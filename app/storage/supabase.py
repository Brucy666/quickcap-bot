# app/storage/supabase.py
from __future__ import annotations
import aiohttp
import asyncio
import json
from typing import Any, Dict, List, Optional

from app.logger import get_logger

log = get_logger("supabase")

class Supa:
    def __init__(self, url: str, key: str):
        if not url or not key:
            raise ValueError("Supabase URL and Key must be provided")
        self.url = url.rstrip("/")
        self.key = key
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

    async def _post(self, table: str, payload: Dict[str, Any]) -> None:
        await self._ensure_session()
        url = f"{self.url}/rest/v1/{table}"
        try:
            async with self._session.post(
                url, headers=self.headers, json=payload, timeout=10
            ) as resp:
                if resp.status >= 300:
                    text = await resp.text()
                    log.error(f"[SUPABASE] {table} insert failed {resp.status}: {text}")
        except Exception as e:
            log.exception(f"[SUPABASE] post error for {table}: {e}")

    async def _upsert(self, table: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
        await self._ensure_session()
        url = f"{self.url}/rest/v1/{table}?on_conflict={on_conflict}"
        try:
            async with self._session.post(
                url, headers=self.headers, json=rows, timeout=15
            ) as resp:
                if resp.status >= 300:
                    text = await resp.text()
                    log.error(f"[SUPABASE] bulk upsert {table} failed {resp.status}: {text}")
        except Exception as e:
            log.exception(f"[SUPABASE] bulk upsert error for {table}: {e}")

    # ----------------- Public API -----------------

    async def log_signal(self, **row: Any) -> None:
        """Insert one signal row."""
        await self._post("signals", row)

    async def log_execution(self, **row: Any) -> None:
        """Insert one execution row."""
        await self._post("executions", row)

    async def bulk_insert(self, table: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
        """Upsert multiple rows with conflict resolution."""
        if not rows:
            return
        await self._upsert(table, rows, on_conflict)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
