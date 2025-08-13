import pandas as pd

def momentum_pop(close: pd.Series, lookback: int = 20, z: float = 2.0) -> pd.Series:
    ret = close.pct_change()
    mu = ret.rolling(lookback).mean()
    sd = ret.rolling(lookback).std().replace(0, 1e-12)
    zscore = (ret - mu) / sd
    return (zscore > z)  # boolean Series
