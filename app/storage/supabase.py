# app/storage/supabase.py
from __future__ import annotations

import aiohttp
import json
from typing import Any, Dict, Iterable, Optional

class Supa:
    """
    Minimal Supabase REST client (PostgREST).
    - signals: upsert on (venue,symbol,interval,ts)
    - executions: plain insert (no on_conflict required)
    - ignores duplicate / 409 errors gracefully
    - single shared aiohttp session
    """

    def __init__(self, url: str, key: str, schema: str = "public", timeout: int = 15) -> None:
        if not url or not key:
            raise ValueError("Supabase URL/Key required")
        base = url.rstrip("/")
        if not base.endswith("/rest/v1"):
            base = f"{base}/rest/v1"
        self.base = base
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

    # ---------- session ----------

    async def _session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ---------- HTTP helpers ----------

    async def _post(
        self,
        table: str,
        rows: Iterable[Dict[str, Any]],
        *,
        prefer: str = "return=minimal",
        on_conflict: Optional[str] = None,
    ) -> None:
        sess = await self._session()
        url = f"{self.base}/{table}"
        if on_conflict:
            url = f"{url}?on_conflict={on_conflict}"

        headers = dict(self._base_headers)
        headers["Prefer"] = prefer

        payload = json.dumps(list(rows), ensure_ascii=False)
        async with sess.post(url, data=payload, headers=headers) as resp:
            if resp.status in (200, 201, 204):
                return
            if resp.status == 409:
                # duplicate / conflict – OK to ignore for idempotency
                return
            txt = await resp.text()
            raise RuntimeError(f"Supabase POST {table} failed [{resp.status}]: {txt}")

    # ---------- shaping ----------

    @staticmethod
    def _shape_signal(row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row)

        # Ensure 'close' column exists (some schemas require it)
        if "close" not in out and "price" in out:
            out["close"] = out["price"]

        # Normalize triggers -> list
        tr = out.get("triggers")
        if tr is None:
            out["triggers"] = []
        elif isinstance(tr, str):
            out["triggers"] = [t.strip() for t in tr.split(",") if t.strip()]
        elif isinstance(tr, (list, tuple)):
            out["triggers"] = list(tr)
        else:
            out["triggers"] = [str(tr)]

        # Your 'signals' table does NOT have these columns — strip them always
        out.pop("basis_pct", None)
        out.pop("basis_z", None)

        return out

    # ---------- public API ----------

    async def log_signal(self, **row: Any) -> None:
        shaped = self._shape_signal(row)
        # proper upsert on signals unique key
        await self._post(
            "signals",
            [shaped],
            prefer="resolution=merge-duplicates,return=minimal",
            on_conflict="venue,symbol,interval,ts",
        )

    async def log_execution(self, **row: Any) -> None:
        # plain insert (no on_conflict; your table lacks a matching unique index)
        await self._post(
            "executions",
            [row],
            prefer="return=minimal",
            on_conflict=None,
        )

    async def bulk_insert(
        self,
        table: str,
        rows: Iterable[Dict[str, Any]],
        *,
        on_conflict: Optional[str] = None,
        prefer_return: str = "minimal",
    ) -> None:
        await self._post(
            table,
            rows,
            prefer=f"return={prefer_return}" if not on_conflict else f"resolution=merge-duplicates,return={prefer_return}",
            on_conflict=on_conflict,
        )
