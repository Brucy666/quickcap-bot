# app/backtest/metrics.py  (full file)
from datetime import datetime, timezone
import pandas as pd
from app.utils import to_dataframe
from app.storage.sqlite_store import SQLiteStore
from app.exchanges import KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic

SPOT = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}

HORIZONS_MIN = (15, 30, 60)

def _dir(side: str) -> int:
    return 1 if str(side).upper() == "LONG" else -1

def _nearest_index(df: pd.DataFrame, ts_ms: int) -> int:
    arr = (df["ts"].astype("int64") // 10**6).to_numpy()
    lo, hi = 0, len(arr) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if arr[mid] < ts_ms: lo = mid + 1
        else: hi = mid - 1
    return lo if lo < len(arr) else -1

def _calc_window(df: pd.DataFrame, i_entry: int, bars: int, side: str, entry: float):
    d = _dir(side)
    i1 = min(len(df)-1, i_entry + max(1, bars))
    seg = df.iloc[i_entry:i1+1]
    exit_price = float(seg.iloc[-1]["close"])
    ret = (exit_price / entry - 1.0) * d
    hi = float(seg["high"].max()); lo = float(seg["low"].min())
    if d > 0:
        mfe = max(0.0, hi / entry - 1.0)
        mae = max(0.0, entry / lo - 1.0)
    else:
        mfe = max(0.0, entry / lo - 1.0)
        mae = max(0.0, hi / entry - 1.0)
    return ret, mfe, mae, exit_price

async def compute_outcomes_sqlite_rows(venue: str, symbol: str, interval: str, lookback: int, store: SQLiteStore):
    ex = SPOT[venue]()
    df = to_dataframe(await ex.fetch_klines(symbol, interval, lookback))
    if len(df) < 5: return []

    with store._conn() as con:
        rows = con.execute(
            "SELECT id, ts, side FROM signals WHERE venue=? AND symbol=? AND interval=? ORDER BY ts ASC",
            (venue, symbol, interval)
        ).fetchall()

    out = []
    step = 1 if interval.endswith("m") else 60
    for sid, ts_iso, side in rows:
        ts_ms = int(datetime.fromisoformat(ts_iso.replace("Z","+00:00")).timestamp() * 1000)
        i0 = _nearest_index(df, ts_ms)
        if i0 < 0: continue
        i_entry = min(i0 + 1, len(df)-1)           # next-bar-open
        entry = float(df.iloc[i_entry]["open"])
        for h in HORIZONS_MIN:
            bars = max(1, h // step)
            ret, mfe, mae, exit_price = _calc_window(df, i_entry, bars, side, entry)
            out.append({
                "signal_id": int(sid),
                "horizon_m": int(h),
                "entry_price": float(entry),
                "exit_price": float(exit_price),
                "ret": float(ret),
                "max_fav": float(mfe),
                "max_adv": float(mae),
            })
    store.upsert_outcomes(out)
    return out
