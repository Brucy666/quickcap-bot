import pandas as pd

def score_row(row: pd.Series) -> float:
    score = 0.0
    # VWAP sweep: close crosses above/below vwap within last bar
    if row.get("sweep_long", False):
        score += 2.0
    if row.get("sweep_short", False):
        score += 2.0
    # Divergences
    if row.get("bull_div", False):
        score += 1.5
    if row.get("bear_div", False):
        score += 1.5
    # Momentum
    if row.get("mom_pop", False):
        score += 1.0
    # Distance to VWAP (tighter is better)
    if "vwap" in row and "close" in row:
        dist = abs(row["close"] - row["vwap"]) / row["vwap"] * 100
        score += max(0, 1.0 - min(dist, 1.0))  # 0..1
    return round(score, 3)
