import pandas as pd

def find_rsi_divergences(df: pd.DataFrame, swing: int = 3):
    # Simple pivot-based divergence detection (HL/LH vs RSI)
    piv_high = (df["high"].shift(1) > df["high"].shift(swing)) & (df["high"].shift(1) > df["high"].shift(-swing))
    piv_low  = (df["low"].shift(1)  < df["low"].shift(swing))  & (df["low"].shift(1)  < df["low"].shift(-swing))

    # Explicit cast to bool to avoid FutureWarning
    df["_ph"] = piv_high.shift(-1).fillna(False).astype(bool)
    df["_pl"] = piv_low.shift(-1).fillna(False).astype(bool)

    bears, bulls = [], []
    last_ph = df.index[df["_ph"]].tolist()
    last_pl = df.index[df["_pl"]].tolist()

    for i in range(1, len(last_ph)):
        a, b = last_ph[i-1], last_ph[i]
        if df.loc[b, "high"] > df.loc[a, "high"] and df.loc[b, "rsi"] < df.loc[a, "rsi"]:
            bears.append(b)

    for i in range(1, len(last_pl)):
        a, b = last_pl[i-1], last_pl[i]
        if df.loc[b, "low"] < df.loc[a, "low"] and df.loc[b, "rsi"] > df.loc[a, "rsi"]:
            bulls.append(b)

    df.drop(columns=["_ph", "_pl"], inplace=True)
    return set(bulls), set(bears)
