# app/storage/supabase.py
# Minimal async Supabase (PostgREST) client for inserts/upserts.
# Uses service_role key (server-side). Safe for Railway.
from __future__ import annotations
import json
from typing import Any, Dict, Iterable, List, Optional, Union
import aiohttp

Row = Dict[str, Any]

class Supa:
    def __init__(self, url: str, service_key: str, timeout: int = 20):
        if not url.endswith("/"):
            url += "/"
        self.base = url + "rest/v1/"
        self.key = service_key
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    # ---------- public convenience ----------
    async def log_signal(self, **row: Any) -> Row:
        """Insert one row into signals; accepts aliases for timestamp etc."""
        row = self._normalize_signal_row(row)
        return await self.insert("signals", row)

    async def log_execution(self, **row: Any) -> Row:
        """Insert one row into executions; accepts aliases for timestamp etc."""
        row = self._normalize_ts_alias(row)
        return await self.insert("executions", row)

    async def insert(self, table: str, rows: Union[Row, List[Row]]) -> Row:
        """Insert 1..N rows; returns representation from PostgREST."""
        payload = rows if isinstance(rows, list) else [rows]
        return await self._post(table, payload, prefer="return=representation")

    async def bulk_insert(self, table: str, rows: Iterable[Row], on_conflict: Optional[str] = None) -> Row:
        """Bulk insert; optional upsert on_conflict='col1,col2'."""
        batch = list(rows)
        prefer = "return=representation"
        params = {}
        if on_conflict:
            prefer = "resolution=merge-duplicates,return=representation"
            params["on_conflict"] = on_conflict
        return await self._post(table, batch, prefer=prefer, params=params)

    async def upsert(self, table: str, rows: Union[Row, List[Row]], on_conflict: str) -> Row:
        """Upsert rows with given conflict columns (CSV)."""
        payload = rows if isinstance(rows, list) else [rows]
        return await self._post(
            table,
            payload,
            prefer="resolution=merge-duplicates,return=representation",
            params={"on_conflict": on_conflict},
        )

    # ---------- internal ----------
    async def _post(self, table: str, payload: List[Row], prefer: str, params: Optional[Dict[str, str]] = None) -> Row:
        url = self.base + table
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        }
        async with aiohttp.ClientSession(timeout=self.timeout) as sess:
            async with sess.post(url, headers=headers, params=params or {}, data=json.dumps(payload)) as r:
                text = await r.text()
                if r.status >= 400:
                    raise RuntimeError(f"Supabase {table} insert failed [{r.status}]: {text}")
                # PostgREST returns a JSON array for return=representation
                try:
                    data = json.loads(text)
                except Exception:
                    data = {"ok": True, "raw": text}
                return data

    # ---------- normalization helpers ----------
    def _normalize_ts_alias(self, row: Row) -> Row:
        # Accept 'ts', 'timestamp', 'ts_iso' → store as 'ts'
        if "ts" not in row:
            if "timestamp" in row:
                row["ts"] = row.pop("timestamp")
            elif "ts_iso" in row:
                row["ts"] = row.pop("ts_iso")
        # Ensure JSON-friendly types
        if "triggers" in row and isinstance(row["triggers"], (set, tuple)):
            row["triggers"] = list(row["triggers"])
        return row

    def _normalize_signal_row(self, row: Row) -> Row:
        row = self._normalize_ts_alias(row)
        # default fields if missing; Postgres will coerce types
        row.setdefault("signal_type", "spot")
        # Keep triggers as JSON (array/object); PostgREST accepts native JSON
        return row
