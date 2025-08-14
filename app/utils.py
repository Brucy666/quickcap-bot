import pandas as pd
import numpy as np

def to_dataframe(klines: list[list]) -> pd.DataFrame:
    # Expect: [ [ts, open, high, low, close, volume], ... ]
    df = pd.DataFrame(klines, columns=["ts","open","high","low","close","volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna().reset_index(drop=True)

def pct_change(a: float, b: float) -> float:
    return (b - a) / a * 100.0 if a else 0.0
