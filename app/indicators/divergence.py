import pandas as pd

def find_rsi_divergences(df: pd.DataFrame, swing: int = 3):
    """Pivot-based RSI divergences with explicit boolean dtypes (no FutureWarnings)."""
    piv_high = (df["high"].shift(1) > df["high"].shift(swing)) & (df["high"].shift(1) > df["high"].shift(-swing))
    piv_low  = (df["low"].shift(1)  < df["low"].shift(swing))  & (df["low"].shift(1)  < df["low"].shift(-swing))

    df["_ph"] = piv_high.shift(-1).fillna(False).astype(bool)
    df["_pl"] = piv_low.shift(-1).fillna(False).astype(bool)

    bears, bulls = [], []
    ph_idx = df.index[df["_ph"]].tolist()
    pl_idx = df.index[df["_pl"]].tolist()

    for i in range(1, len(ph_idx)):
        a, b = ph_idx[i-1], ph_idx[i]
        if df.loc[b, "high"] > df.loc[a, "high"] and df.loc[b, "rsi"] < df.loc[a, "rsi"]:
            bears.append(b)

    for i in range(1, len(pl_idx)):
        a, b = pl_idx[i-1], pl_idx[i]
        if df.loc[b, "low"] < df.loc[a, "low"] and df.loc[b, "rsi"] > df.loc[a, "rsi"]:
            bulls.append(b)

    df.drop(columns=["_ph", "_pl"], inplace=True)
    return set(bulls), set(bears)
