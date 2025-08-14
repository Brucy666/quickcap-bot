import pandas as pd
from typing import Tuple, Dict, Any
from app.indicators import rsi, session_vwap

def _align(spot: pd.DataFrame, perp: pd.DataFrame, tol_sec: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
    s = spot.copy(); p = perp.copy()
    s = s.sort_values("ts").reset_index(drop=True)
    p = p.sort_values("ts").reset_index(drop=True)
    # Inner-join on nearest timestamp within tolerance
    s["key"] = s["ts"].astype("int64") // 10**9
    p["key"] = p["ts"].astype("int64") // 10**9
    merged = pd.merge_asof(
        s.sort_values("key"), p.sort_values("key"),
        on="key", tolerance=tol_sec, direction="nearest",
        suffixes=("_spot","_perp")
    )
    merged = merged.dropna(subset=["close_spot","close_perp"])
    spot2 = pd.DataFrame({
        "ts": pd.to_datetime(merged["ts_spot"]),
        "open": merged["open_spot"], "high": merged["high_spot"],
        "low": merged["low_spot"], "close": merged["close_spot"], "volume": merged["volume_spot"]
    })
    perp2 = pd.DataFrame({
        "ts": pd.to_datetime(merged["ts_perp"]),
        "open": merged["open_perp"], "high": merged["high_perp"],
        "low": merged["low_perp"], "close": merged["close_perp"], "volume": merged["volume_perp"]
    })
    return spot2.reset_index(drop=True), perp2.reset_index(drop=True)

def compute_basis_signals(spot: pd.DataFrame, perp: pd.DataFrame, z_win: int = 50, z_th: float = 2.5) -> Dict[str, Any]:
    s, p = _align(spot, perp)
    if len(s) < max(50, z_win+5): return {"ok": False, "reason": "insufficient_aligned"}

    s["vwap"] = session_vwap(s)
    s["rsi"] = rsi(s["close"], 14)

    basis = (p["close"] - s["close"]) / (s["close"] + 1e-12) * 100.0
    z = (basis - basis.rolling(z_win).mean()) / (basis.rolling(z_win).std().replace(0, 1e-12))

    last = len(s) - 1
    out = {
        "ok": True,
        "basis_pct": float(basis.iloc[last]),
        "basis_z": float(z.iloc[last]),
        "spot_close": float(s["close"].iloc[last]),
        "spot_vwap": float(s["vwap"].iloc[last]),
        "spot_rsi": float(s["rsi"].iloc[last]),
        "triggers": [],
        "side": None,
    }

    perp_premium = z.iloc[last] >= z_th
    perp_discount = z.iloc[last] <= -z_th
    above_vwap = s["close"].iloc[last] > s["vwap"].iloc[last]
    below_vwap = s["close"].iloc[last] < s["vwap"].iloc[last]

    # High-conviction patterns
    if perp_premium and above_vwap and s["rsi"].iloc[last] >= 60:
        out["triggers"].append("Perp Premium Blowoff")
        out["side"] = "SHORT"
    if perp_discount and below_vwap and s["rsi"].iloc[last] <= 40:
        out["triggers"].append("Perp Discount Capitulation")
        out["side"] = "LONG"

    # Secondary patterns
    if perp_premium and s["rsi"].iloc[last] < 50:
        out["triggers"].append("Premium + RSI Reversal Risk")
        out.setdefault("side", "SHORT")
    if perp_discount and s["rsi"].iloc[last] > 50:
        out["triggers"].append("Discount + RSI Reversal Risk")
        out.setdefault("side", "LONG")

    return out
