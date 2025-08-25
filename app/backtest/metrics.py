# app/backtest/metrics.py
from datetime import datetime, timezone
from typing import Dict, Tuple, List
import pandas as pd

from app.utils import to_dataframe
from app.storage.sqlite_store import SQLiteStore
from app.exchanges import (
    KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic
)

SPOT = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}

HORIZONS_MIN = (15, 30, 60)

def _dir(side: str) -> int:
    return 1 if side.upper() == "LONG" else -1

def _nearest_index(df: pd.DataFrame, ts_ms: int) -> int:
    arr = df["ts"].astype("int64") // 10**6
    for i, t in enumerate(arr):
        if t >= ts_ms:
            return i
    return -1

def _calc_window(df, i0: int, bars: int, side: str, entry: float):
    d = _dir(side)
    i1 = min(len(df)-1, i0 + max(1, bars))
    seg = df.iloc[i0:i1+1]
    exit_price = float(seg.iloc[-1]["close"])
    ret = (exit_price / entry - 1.0) * d

    hi = float(seg["high"].max())
    lo = float(seg["low"].min())
    if d > 0:
        mfe = (hi / entry - 1.0)
        mae = (entry / lo - 1.0)
    else:
        mfe = (entry / lo - 1.0)
        mae = (hi / entry - 1.0)
    return ret, mfe, mae, exit_price

async def compute_outcomes_sqlite_rows(venue: str, symbol: str, interval: str, lookback: int, store: SQLiteStore):
    """Fetch klines, compute horizons for all signals of (venue,symbol,interval).
       Upserts into SQLite and returns the computed rows for mirroring to Supabase."""
    cls = SPOT.get(venue); ex = cls()
    kl = await ex.fetch_klines(symbol, interval, lookback)
    df = to_dataframe(kl)
    if len(df) == 0:
        return []

    with store._conn() as con:
        cur = con.execute(
            "SELECT id, ts, side FROM signals WHERE venue=? AND symbol=? AND interval=? ORDER BY ts ASC",
            (venue, symbol, interval)
        )
        rows = cur.fetchall()

    out = []
    for sid, ts_iso, side in rows:
        ts_ms = int(datetime.fromisoformat(ts_iso.replace("Z","+00:00")).timestamp() * 1000)
        i0 = _nearest_index(df, ts_ms)
        if i0 < 0:
            continue
        entry = float(df.iloc[i0]["close"])
        for h in HORIZONS_MIN:
            bars = h // (1 if interval.endswith("m") else 60)
            ret, mfe, mae, exit_price = _calc_window(df, i0, bars, side, entry)
            out.append({
                "signal_id": int(sid),
                "horizon_m": int(h),
                "entry_price": float(entry),
                "exit_price": float(exit_price),
                "ret": float(ret),
                "max_fav": float(mfe),
                "max_adv": float(mae),
            })
    # persist locally
    store.upsert_outcomes(out)
    return out
