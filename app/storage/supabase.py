# app/storage/supabase.py
from __future__ import annotations

import aiohttp
import json
from typing import Any, Dict, List, Optional


class Supa:
    """
    Minimal, resilient Supabase REST client for logging signals/executions/outcomes.
    - Reuses a single aiohttp session
    - Throws clear errors when REST says 4xx/5xx
    - Only sends known columns
    """

    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.key = key
        self._session: Optional[aiohttp.ClientSession] = None

        # Static headers for REST
        self._headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            # Return minimal body to reduce bandwidth
            "Prefer": "return=minimal",
        }

    # ---------- session lifecycle ----------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """
        Guarantee we have an open aiohttp session.
        (Fixes the 'NoneType is not callable' crash you saw.)
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self._headers)
        return self._session

    async def close(self) -> None:
        s = self._session
        if s and not s.closed:
            await s.close()
        self._session = None

    # ---------- core REST helpers ----------

    async def _post_rows(self, table: str, rows: List[Dict[str, Any]]) -> None:
        """
        Insert a list of rows using Supabase REST bulk endpoint (row-by-row JSON).
        We send only fields present in each row — Supabase will ignore missing columns.
        """
        if not rows:
            return

        s = await self._ensure_session()
        url = f"{self.url}/rest/v1/{table}"
        # Supabase REST accepts an array of JSON objects for bulk insert.
        payload = json.dumps(rows)

        async with s.post(url, data=payload) as r:
            if r.status >= 300:
                txt = await r.text()
                raise RuntimeError(f"Supabase POST {table} failed [{r.status}]: {txt}")

    # ---------- public logging API ----------

    # NOTE: We **whitelist** safe columns to avoid “missing column” schema errors.
    _SIGNAL_COLS = {
        "ts", "signal_type", "venue", "symbol", "interval",
        "side", "price", "vwap", "rsi", "score", "triggers"
    }
    _EXEC_COLS = {
        "ts", "venue", "symbol", "side", "price", "score", "reason", "is_paper"
    }

    def _project(self, row: Dict[str, Any], allowed: set[str]) -> Dict[str, Any]:
        # Keep only allowed keys and coerce 'triggers' list -> JSON string for safety
        out = {k: row[k] for k in row.keys() & allowed}
        if "triggers" in out and isinstance(out["triggers"], list):
            out["triggers"] = json.dumps(out["triggers"])
        return out

    async def log_signal(self, **fields) -> None:
        row = self._project(fields, self._SIGNAL_COLS)
        await self._post_rows("signals", [row])

    async def log_execution(self, **fields) -> None:
        row = self._project(fields, self._EXEC_COLS)
        await self._post_rows("executions", [row])

    async def bulk_insert(self, table: str, rows: List[Dict[str, Any]], on_conflict: str = "") -> None:
        # Generic bulk insert — used for outcomes. Don’t assume any schema.
        await self._post_rows(table, rows)
