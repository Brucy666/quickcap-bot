# app/backtest/metrics.py  (no network; computes outcomes from an existing DF)
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Dict
import pandas as pd

from app.storage.sqlite_store import SQLiteStore

HORIZONS_MIN: tuple[int, ...] = (15, 30, 60)


# --------- helpers ---------

def _dir(side: str) -> int:
    return 1 if str(side).upper() == "LONG" else -1


def _ts_ms(x) -> int:
    """
    Normalize a timestamp-like (pd.Timestamp or ISO string) to epoch ms (int).
    """
    if isinstance(x, pd.Timestamp):
        # pandas already tz-aware/naive; use .tz_localize(None) safely
        return int(x.tz_localize(None).timestamp() * 1000) if x.tzinfo else int(x.timestamp() * 1000)
    if isinstance(x, str):
        # Accept '...Z' or ISO with offset
        dt = datetime.fromisoformat(x.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    # fallback (assume epoch seconds)
    return int(float(x) * 1000)


def _nearest_index(df: pd.DataFrame, ts_ms: int) -> int:
    """
    Lower_bound on df['ts'] (epoch ms) to get the candle containing/after ts_ms.
    Returns index in [0, len) or -1 when off the end.
    """
    arr = (df["ts"].astype("int64") // 10**6).to_numpy()
    lo, hi = 0, len(arr) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if arr[mid] < ts_ms:
            lo = mid + 1
        else:
            hi = mid - 1
    return lo if lo < len(arr) else -1


def _calc_window(
    df: pd.DataFrame,
    i_entry: int,
    bars: int,
    side: str,
    entry: float,
) -> tuple[float, float, float, float]:
    """
    Compute ret, MFE, MAE, exit_price from i_entry over `bars` candles.
    """
    d = _dir(side)
    i1 = min(len(df) - 1, i_entry + max(1, bars))
    seg = df.iloc[i_entry : i1 + 1]

    exit_price = float(seg.iloc[-1]["close"])
    ret = (exit_price / entry - 1.0) * d

    hi = float(seg["high"].max())
    lo = float(seg["low"].min())

    if d > 0:
        mfe = max(0.0, hi / entry - 1.0)
        mae = max(0.0, entry / lo - 1.0)
    else:
        mfe = max(0.0, entry / lo - 1.0)
        mae = max(0.0, hi / entry - 1.0)

    return ret, mfe, mae, exit_price


# --------- public API (preferred) ---------

def compute_outcomes_from_df(
    df: pd.DataFrame,
    venue: str,
    symbol: str,
    interval: str,
    store: SQLiteStore,
    horizons: Iterable[int] = HORIZONS_MIN,
) -> List[Dict]:
    """
    Read signals for (venue, symbol, interval) from SQLite, then compute outcome rows
    using the **existing candles dataframe `df`** (no network calls).
    Upserts to SQLite via store.upsert_outcomes(...) and returns the rows.
    """
    if df is None or df.empty:
        return []

    # Ensure we have the columns we need and ts is monotonic
    required_cols = {"ts", "open", "high", "low", "close"}
    if not required_cols.issubset(set(df.columns)):
        raise ValueError(f"compute_outcomes_from_df: df missing columns {required_cols - set(df.columns)}")

    df = df.reset_index(drop=True).copy()
    df.sort_values("ts", inplace=True, ignore_index=True)

    # Fetch signals in time order
    with store._conn() as con:
        sig_rows = con.execute(
            """
            SELECT id, ts, side
            FROM signals
            WHERE venue = ? AND symbol = ? AND interval = ?
            ORDER BY ts ASC
            """,
            (venue, symbol, interval),
        ).fetchall()

    if not sig_rows:
        return []

    out: List[Dict] = []
    # step in bars per minute-candle; for 1m bars => 1, for 5m bars => 5, etc.
    # crude but works for m-intervals
    try:
        step = int(interval.rstrip("m"))
    except Exception:
        step = 1

    for sid, ts_iso, side in sig_rows:
        ts_ms = _ts_ms(ts_iso)
        i0 = _nearest_index(df, ts_ms)
        if i0 < 0:
            continue

        # entry at next bar open (classic "no-lookahead" fill)
        i_entry = min(i0 + 1, len(df) - 1)
        entry = float(df.iloc[i_entry]["open"])

        for h in horizons:
            bars = max(1, int(h // step))
            ret, mfe, mae, exit_price = _calc_window(df, i_entry, bars, side, entry)
            out.append(
                {
                    "signal_id": int(sid),
                    "horizon_m": int(h),
                    "entry_price": float(entry),
                    "exit_price": float(exit_price),
                    "ret": float(ret),
                    "max_fav": float(mfe),
                    "max_adv": float(mae),
                }
            )

    if out:
        store.upsert_outcomes(out)
    return out


# --------- compatibility wrapper (optional) ---------

async def compute_outcomes_sqlite_rows(
    venue: str,
    symbol: str,
    interval: str,
    lookback: int,
    store: SQLiteStore,
    df: pd.DataFrame | None = None,
):
    """
    Backward-compatible wrapper. Prefer calling compute_outcomes_from_df(...)
    and pass `df` from your caller. If df is None, returns [] (no network).
    """
    if df is None:
        # We intentionally avoid fetching candles here to eliminate timeouts.
        # Callers should pass the already-fetched dataframe.
        return []
    return compute_outcomes_from_df(df, venue, symbol, interval, store, horizons=HORIZONS_MIN)
