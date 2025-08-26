import pandas as pd

def score_row(row: pd.Series) -> float:
    """
    Compute a composite score for a signal row.
    Emphasis:
      - Event-type sweeps/divergences at VWAP
      - Capitulatory 'mom_pop' plays weaker
      - VWAP distance scaling for divergences
    Returns float in range [0, 10].
    """
    score = 0.0

    # --- VWAP sweep: event-type triggers ---
    if row.get("sweep_long", False):
        score += 2.5   # event bias stronger
    if row.get("sweep_short", False):
        score += 2.5

    # --- Divergences ---
    vwap_dist_bonus = 0.0
    if "vwap" in row and "close" in row and row["vwap"] > 0:
        dist = abs(row["close"] - row["vwap"]) / row["vwap"] * 100  # percent
        # closer = more edge, max +1.0 if <0.25%
        vwap_dist_bonus = max(0, 1.0 - min(dist / 0.25, 1.0))

    if row.get("bull_div", False):
        score += 1.8 + 0.5 * vwap_dist_bonus
    if row.get("bear_div", False):
        score += 1.8 + 0.5 * vwap_dist_bonus

    # --- Momentum pop (weaker edge) ---
    if row.get("mom_pop", False):
        score += 0.6   # downweighted vs before

    # --- Generic VWAP proximity (all plays better when near VWAP) ---
    score += vwap_dist_bonus * 0.7

    # --- Clip and round ---
    score = max(0.0, min(10.0, score))
    return round(score, 3)
