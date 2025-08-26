# app/storage/supabase.py
from __future__ import annotations

from typing import Iterable, Optional, Sequence, Dict, Any


def _ensure_jsonable(v: Any) -> Any:
    """
    Supabase will happily take Python lists/dicts for JSON/JSONB columns and
    Postgres arrays. Keep payloads simple (lists, numbers, strings, bools).
    """
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (list, tuple)):
        return [_ensure_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _ensure_jsonable(v[k]) for k in v}
    # Fallback to string representation for exotic types
    return str(v)


class Supa:
    """
    Thin helper around the supabase-python client. All calls are synchronous
    (client is sync), so it's safe to invoke from async code.
    """
    def __init__(self, url: str, key: str):
        from supabase import create_client  # local import to avoid hard dep at import time
        self._client = create_client(url, key)

    # --------------- single row writers ---------------

    def log_signal(self, **row) -> None:
        """
        Expected columns in `signals`:
          ts (timestamptz), signal_type, venue, symbol, interval, side,
          price, vwap, rsi, score, triggers (json/array), signal_key (text, unique)
        """
        payload = dict(row)
        if "triggers" in payload:
            payload["triggers"] = _ensure_jsonable(payload["triggers"])
        # Upsert on signal_key so backfills & repeats don't duplicate
        self._client.table("signals").upsert(payload, on_conflict="signal_key").execute()

    def log_execution(self, **row) -> None:
        """
        Insert into executions. Typical columns:
          ts, venue, symbol, side, price, score, reason, is_paper
        """
        payload = dict(row)
        self._client.table("executions").insert(payload).execute()

    # --------------- bulk writer ---------------

    def bulk_insert(self, table: str, rows: Sequence[Dict[str, Any]], on_conflict: Optional[str] = None) -> None:
        """
        Bulk insert/upsert. For outcomes we usually pass on_conflict="signal_key,horizon_m".
        """
        if not rows:
            return
        clean = []
        for r in rows:
            rr = {k: _ensure_jsonable(v) for k, v in r.items()}
            clean.append(rr)
        if on_conflict:
            self._client.table(table).upsert(clean, on_conflict=on_conflict).execute()
        else:
            self._client.table(table).insert(clean).execute()
