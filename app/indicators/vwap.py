import pandas as pd

def session_vwap(df: pd.DataFrame) -> pd.Series:
    # Session = UTC day. Resets at 00:00 UTC.
    day = df["ts"].dt.floor("D")
    pv = (df["close"] * df["volume"]).groupby(day).cumsum()
    vv = df["volume"].groupby(day).cumsum() + 1e-12
    return pv / vv
