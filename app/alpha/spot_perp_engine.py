import pandas as pd
from typing import Tuple, Dict, Any
from app.indicators import rsi, session_vwap

def _align(spot: pd.DataFrame, perp: pd.DataFrame, tol_sec: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Align spot & perp by nearest timestamp within tolerance (seconds)."""
    s = spot.copy().sort_values("ts").reset_index(drop=True)
    p = perp.copy().sort_values("ts").reset_index(drop=True)
    s["key"] = s["ts"].astype("int64") // 10**9
    p["key"] = p["ts"].astype("int64") // 10**9
    merged = pd.merge_asof(
        s.sort_values("key"), p.sort_values("key"),
        on="key", tolerance=tol_sec, direction="nearest",
        suffixes=("_spot","_perp")
    ).dropna(subset=["close_spot","close_perp"])

    s2 = pd.DataFrame({
        "ts": pd.to_datetime(merged["ts_spot"]),
        "open": merged["open_spot"], "high": merged["high_spot"],
        "low": merged["low_spot"], "close": merged["close_spot"], "volume": merged["volume_spot"]
    })
    p2 = pd.DataFrame({
        "ts": pd.to_datetime(merged["ts_perp"]),
        "open": merged["open_perp"], "high": merged["high_perp"],
        "low": merged["low_perp"], "close": merged["close_perp"], "volume": merged["volume_perp"]
    })
    return s2.reset_index(drop=True), p2.reset_index(drop=True)

def compute_basis_signals(spot: pd.DataFrame, perp: pd.DataFrame, z_win: int = 50, z_th: float = 2.5) -> Dict[str, Any]:
    """
    Compute perp premium/discount basis z-score vs spot and gate with VWAP + RSI.
    Returns dict with fields: ok, basis_pct, basis_z, spot_close, spot_vwap, spot_rsi, triggers, side.
    """
    s, p = _align(spot, perp)
    if len(s) < max(50, z_win + 5):
        return {"ok": False, "reason": "insufficient_aligned"}

    s["vwap"] = session_vwap(s)
    s["rsi"] = rsi(s["close"], 14)

    basis = (p["close"] - s["close"]) / (s["close"] + 1e-12) * 100.0
    mu = basis.rolling(z_win).mean()
    sd = basis.rolling(z_win).std().replace(0, 1e-12)
    z = (basis - mu) / sd

    i = len(s) - 1
    out = {
        "ok": True,
        "basis_pct": float(basis.iloc[i]),
        "basis_z": float(z.iloc[i]),
        "spot_close": float(s["close"].iloc[i]),
        "spot_vwap": float(s["vwap"].iloc[i]),
        "spot_rsi": float(s["rsi"].iloc[i]),
        "triggers": [],
        "side": None,
    }

    perp_premium  = z.iloc[i] >= z_th          # perp > spot (premium)
    perp_discount = z.iloc[i] <= -z_th         # perp < spot (discount)
    above_vwap = s["close"].iloc[i] > s["vwap"].iloc[i]
    below_vwap = s["close"].iloc[i] < s["vwap"].iloc[i]
    rsi_val = s["rsi"].iloc[i]

    # High-conviction patterns
    if perp_premium and above_vwap and rsi_val >= 60:
        out["triggers"].append("Perp Premium Blowoff")
        out["side"] = "SHORT"
    if perp_discount and below_vwap and rsi_val <= 40:
        out["triggers"].append("Perp Discount Capitulation")
        out["side"] = "LONG"

    # Secondary patterns (potential reversal context)
    if perp_premium and rsi_val < 50:
        out["triggers"].append("Premium + RSI Reversal Risk")
        out.setdefault("side", "SHORT")
    if perp_discount and rsi_val > 50:
        out["triggers"].append("Discount + RSI Reversal Risk")
        out.setdefault("side", "LONG")

    return out
