# app/storage/supabase.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import aiohttp


class Supa:
    """
    Minimal async Supabase REST client that uses aiohttp (already in your stack).
    - Ensures every signal has a stable `signal_key`
    - Upserts with on_conflict when asked
    - No extra dependencies
    """

    def __init__(
        self,
        url: str,
        key: str,
        *,
        schema: str = "public",
        supports_outcomes: bool = False,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        if not url or not key:
            raise ValueError("Supabase URL and Key are required")
        self.url = url.rstrip("/")
        self.key = key
        self.schema = schema
        self._session = session
        self.supports_outcomes = supports_outcomes  # set True only if your DB uses signal_key FKs

        self._base_headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            # Allow upserts to merge duplicates
            "Prefer": "resolution=merge-duplicates",
        }

    # ---------------- internal helpers ----------------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _post(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        *,
        on_conflict: Optional[str] = None,
        prefer_return: str = "minimal",
    ) -> Dict[str, Any]:
        if not rows:
            return {}
        sess = await self._ensure_session()
        headers = dict(self._base_headers)
        # Return policy: minimal avoids large payloads
        headers["Prefer"] = f"{headers['Prefer']},return={prefer_return}"

        url = f"{self.url}/rest/v1/{table}"
        if on_conflict:
            url += f"?on_conflict={on_conflict}"

        async with sess.post(url, headers=headers, data=json.dumps(rows)) as resp:
            txt = await resp.text()
            if resp.status >= 300:
                raise RuntimeError(f"Supabase POST {table} failed [{resp.status}]: {txt}")
            return json.loads(txt) if txt else {}

    # ---------------- public API ----------------

    async def log_signal(self, **kwargs) -> Dict[str, Any]:
        """
        Insert a signal row into 'signals'.
        Ensures a stable signal_key if caller didn't provide one.
        """
        if "signal_key" not in kwargs or not kwargs["signal_key"]:
            # venue:symbol:ts is stable for backfills & live
            kwargs["signal_key"] = f"{kwargs.get('venue')}:{kwargs.get('symbol')}:{kwargs.get('ts')}"
        return await self._post("signals", [kwargs], prefer_return="minimal")

    async def log_execution(self, **kwargs) -> Dict[str, Any]:
        """
        Insert an execution row into 'executions'.
        """
        return await self._post("executions", [kwargs], prefer_return="minimal")

    async def bulk_insert(
        self,
        table: str,
        rows: List[Dict[str, Any]],
        *,
        on_conflict: Optional[str] = None,
        prefer_return: str = "minimal",
    ) -> Dict[str, Any]:
        """
        Bulk insert/upsert rows into a table.
        Example for outcomes (only if your schema supports signal_key FKs):
          await supa.bulk_insert("signal_outcomes", rows, on_conflict="signal_key,horizon_m")
        """
        return await self._post(table, rows, on_conflict=on_conflict, prefer_return=prefer_return)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
