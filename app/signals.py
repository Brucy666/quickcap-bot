# app/signals.py
import pandas as pd
from app.config import load_settings
from .indicators import rsi, session_vwap, find_rsi_divergences, momentum_pop

def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    cfg = load_settings()
    out = df.copy()
    out["rsi"] = rsi(out["close"], 14)
    out["vwap"] = session_vwap(out)
    bulls, bears = find_rsi_divergences(out)
    out["bull_div"] = out.index.isin(bulls)
    out["bear_div"] = out.index.isin(bears)
    out["sweep_long"]  = (out["low"] <= out["vwap"]) & (out["close"] > out["vwap"])
    out["sweep_short"] = (out["high"] >= out["vwap"]) & (out["close"] < out["vwap"])
    out["mom_pop"] = momentum_pop(out["close"], lookback=20, z=float(cfg.momentum_z))
    return out
