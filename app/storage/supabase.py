# app/storage/supabase.py
from __future__ import annotations

import aiohttp
import asyncio
import json
from typing import Any, Dict, Iterable, Optional

class Supa:
    """
    Minimal Supabase REST client (PostgREST) with idempotent upserts.
    - Upserts via POST + ?on_conflict=... and Prefer: resolution=merge-duplicates
    - Ignores 409 duplicate errors
    - Uses a single aiohttp session
    """

    def __init__(self, url: str, key: str, schema: str = "public", timeout: int = 15) -> None:
        if not url or not key:
            raise ValueError("Supabase URL/Key required")
        # Normalize base URL (expecting .../rest/v1)
        self.base = url.rstrip("/")
        if not self.base.endswith("/rest/v1"):
            self.base = f"{self.base}/rest/v1"
        self.key = key
        self.schema = schema
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

        self._base_headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Accept-Profile": self.schema,
        }

    # ---------- internals ----------

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _post_upsert(
        self,
        table: str,
        rows: Iterable[Dict[str, Any]],
        on_conflict: Optional[str] = None,
        prefer_return: str = "minimal",
    ) -> None:
        """
        Upsert rows into table. Ignores 409 (duplicate) errors.
        """
        sess = await self._session_get()
        # Build URL with on_conflict columns when provided
        url = f"{self.base}/{table}"
        if on_conflict:
            url = f"{url}?on_conflict={on_conflict}"

        headers = dict(self._base_headers)
        headers["Prefer"] = f"resolution=merge-duplicates,return={prefer_return}"

        payload = json.dumps(list(rows), ensure_ascii=False)
        async with sess.post(url, data=payload, headers=headers) as resp:
            if resp.status == 409:
                # duplicate key (expected with concurrent workers) -> ignore
                return
            if resp.status >= 300:
                txt = await resp.text()
                raise RuntimeError(f"Supabase POST {table} failed [{resp.status}]: {txt}")

    # ---------- public helpers ----------

    @staticmethod
    def _normalize_signal_row(row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure schema-friendly payload:
        - 'close' present (auto from 'price' if missing)
        - triggers as JSON-compatible list
        - optional basis fields only when provided
        """
        out = dict(row)  # shallow copy

        # Ensure 'close' column is present for signals table
        if "close" not in out and "price" in out:
            out["close"] = out["price"]

        # Normalize triggers
        tr = out.get("triggers")
        if tr is None:
            out["triggers"] = []
        elif isinstance(tr, str):
            # split simple comma string if accidentally passed
            out["triggers"] = [t.strip() for t in tr.split(",") if t.strip()]
        elif isinstance(tr, (list, tuple)):
            out["triggers"] = list(tr)
        else:
            # fallback to string repr
            out["triggers"] = [str(tr)]

        # Remove None basis fields to avoid schema cache complaints
        for k in ("basis_pct", "basis_z"):
            if out.get(k, None) is None:
                out.pop(k, None)

        return out

    async def log_signal(self, **row: Any) -> None:
        """
        Upsert one row into 'signals'.
        Unique constraint (recommended): (venue, symbol, interval, ts)
        """
        normalized = self._normalize_signal_row(row)
        await self._post_upsert(
            table="signals",
            rows=[normalized],
            on_conflict="venue,symbol,interval,ts",
            prefer_return="minimal",
        )

    async def log_execution(self, **row: Any) -> None:
        """
        Upsert one row into 'executions'.
        Recommended unique key: (venue, symbol, ts, side)
        """
        await self._post_upsert(
            table="executions",
            rows=[row],
            on_conflict="venue,symbol,ts,side",
            prefer_return="minimal",
        )

    async def bulk_insert(
        self,
        table: str,
        rows: Iterable[Dict[str, Any]],
        on_conflict: Optional[str] = None,
        prefer_return: str = "minimal",
    ) -> None:
        # For bulk outcomes etc.
        await self._post_upsert(
            table=table,
            rows=rows,
            on_conflict=on_conflict,
            prefer_return=prefer_return,
        )
