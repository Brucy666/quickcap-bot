# app/storage/supabase.py
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

    # ---------- convenience ----------
    async def log_signal(self, **row: Any) -> Row:
        row = self._normalize_signal_row(row)
        # idempotent on (venue,symbol,interval,ts)
        return await self.upsert_one("signals", row, on_conflict="venue,symbol,interval,ts")

    async def log_execution(self, **row: Any) -> Row:
        row = self._normalize_ts_alias(row)
        # no unique constraint -> plain insert
        return await self.insert("executions", row)

    async def insert(self, table: str, rows: Union[Row, List[Row]]) -> Row:
        payload = rows if isinstance(rows, list) else [rows]
        return await self._post(table, payload, prefer="return=representation", params=None)

    async def bulk_insert(self, table: str, rows: Iterable[Row], on_conflict: Optional[str] = None) -> Row:
        batch = list(rows)
        prefer = "return=representation"
        params: Dict[str,str] = {}
        if on_conflict:
            prefer = "resolution=merge-duplicates,return=representation"
            params["on_conflict"] = on_conflict
        return await self._post(table, batch, prefer=prefer, params=params)

    async def upsert(self, table: str, rows: Union[Row, List[Row]], on_conflict: str) -> Row:
        payload = rows if isinstance(rows, list) else [rows]
        return await self._post(
            table,
            payload,
            prefer="resolution=merge-duplicates,return=representation",
            params={"on_conflict": on_conflict},
        )

    async def upsert_one(self, table: str, row: Row, on_conflict: str) -> Row:
        return await self.upsert(table, row, on_conflict)

    # ---------- internals ----------
    async def _post(self, table: str, payload: List[Row], prefer: str, params: Optional[Dict[str, str]]) -> Row:
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
                try:
                    return json.loads(text)
                except Exception:
                    return {"ok": True, "raw": text}

    # ---------- normalization ----------
    def _normalize_ts_alias(self, row: Row) -> Row:
        if "ts" not in row:
            if "timestamp" in row:
                row["ts"] = row.pop("timestamp")
            elif "ts_iso" in row:
                row["ts"] = row.pop("ts_iso")
        if "triggers" in row and isinstance(row["triggers"], (set, tuple)):
            row["triggers"] = list(row["triggers"])
        return row

    def _normalize_signal_row(self, row: Row) -> Row:
        row = self._normalize_ts_alias(row)
        row.setdefault("signal_type", "spot")
        return row
