# app/storage/supabase.py
from __future__ import annotations
import os, aiohttp, asyncio
from typing import Any, Dict
from app.logger import get_logger

log = get_logger("supabase")

class Supa:
    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.key = key
        self._headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

    async def _post(self, table: str, row: Dict[str, Any]):
        """Insert a single row into Supabase, filtering unknown keys."""
        clean_row = {k: v for k, v in row.items() if v is not None}  # drop nulls
        url = f"{self.url}/rest/v1/{table}"
        async with aiohttp.ClientSession() as s:
            try:
                async with s.post(
                    url, headers=self._headers, json=clean_row, params={"return": "minimal"}
                ) as r:
                    if r.status >= 300:
                        txt = await r.text()
                        log.error(f"Supabase POST {table} failed {r.status}: {txt}")
            except Exception as e:
                log.error(f"Supabase error posting to {table}: {e}")

    async def log_signal(self, **row):
        # Only keep safe columns for signals table
        allowed = {
            "ts", "signal_type", "venue", "symbol", "interval",
            "side", "price", "vwap", "rsi", "score", "triggers"
        }
        clean = {k: row[k] for k in row if k in allowed}
        await self._post("signals", clean)

    async def log_execution(self, **row):
        # Only keep safe columns for executions table
        allowed = {
            "ts", "venue", "symbol", "side",
            "price", "score", "reason", "is_paper"
        }
        clean = {k: row[k] for k in row if k in allowed}
        await self._post("executions", clean)

    async def bulk_insert(self, table: str, rows: list[Dict[str, Any]], on_conflict: str | None = None):
        if not rows:
            return
        clean_rows = []
        for r in rows:
            clean_rows.append({k: v for k, v in r.items() if v is not None})
        url = f"{self.url}/rest/v1/{table}"
        params = {"return": "minimal"}
        if on_conflict:
            params["on_conflict"] = on_conflict
        async with aiohttp.ClientSession() as s:
            try:
                async with s.post(url, headers=self._headers, json=clean_rows, params=params) as r:
                    if r.status >= 300:
                        txt = await r.text()
                        log.error(f"Supabase bulk insert {table} failed {r.status}: {txt}")
            except Exception as e:
                log.error(f"Supabase bulk insert error {table}: {e}")
