# app/storage/supabase.py
from __future__ import annotations
import json
import requests
from typing import Any, Dict, List

class Supa:
    def __init__(self, url: str, key: str, schema: str = "public"):
        if not url or not key:
            raise ValueError("Supabase URL and Key are required")
        self.url = url.rstrip("/")
        self.key = key
        self.schema = schema
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    # ---- low-level helper ----
    def _post(self, table: str, rows: List[Dict[str, Any]]):
        resp = requests.post(
            f"{self.url}/rest/v1/{table}",
            headers=self.headers,
            data=json.dumps(rows),
        )
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase insert failed [{resp.status_code}]: {resp.text}")
        return resp.json() if resp.text else {}

    # ---- signal logging ----
    def log_signal(self, **kwargs):
        """
        Insert a trading signal. Must include `signal_key`.
        """
        if "signal_key" not in kwargs:
            kwargs["signal_key"] = f"{kwargs.get('venue')}:{kwargs.get('symbol')}:{kwargs.get('ts')}"
        return self._post("signals", [kwargs])

    def log_execution(self, **kwargs):
        """
        Insert execution (paper/live). No FK required.
        """
        return self._post("executions", [kwargs])

    def bulk_insert(self, table: str, rows: List[Dict[str, Any]], on_conflict: str | None = None):
        """
        Insert/upsert multiple rows with conflict resolution.
        Example: bulk_insert("signal_outcomes", rows, on_conflict="signal_key,horizon_m")
        """
        if not rows:
            return {}

        url = f"{self.url}/rest/v1/{table}"
        headers = self.headers.copy()
        if on_conflict:
            headers["Prefer"] = f"resolution=merge-duplicates,return=minimal"
            url += f"?on_conflict={on_conflict}"

        resp = requests.post(url, headers=headers, data=json.dumps(rows))
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase bulk_insert failed [{resp.status_code}]: {resp.text}")
        return resp.json() if resp.text else {}
