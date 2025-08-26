# app/tools/aggregate_performance.py
import sqlite3, math, sys, json
import pandas as pd

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "quickcap_results.db"

def read_table(conn, name):
    try:
        return pd.read_sql(f"SELECT * FROM {name}", conn)
    except Exception:
        return pd.DataFrame()

def infer_bucket(reason: str) -> str:
    r = (reason or "").lower()
    if "discount capitulation" in r or "premium blowoff" in r:
        return "event"
    if "rsi" in r or "premium" in r or "discount" in r:
        return "rsi"
    if "momentum pop" in r:
        return "momo"
    return "other"

def first_existing(cols, row, default=None):
    for c in cols:
        if c in row and pd.notna(row[c]):
            return row[c]
    return default

def main():
    conn = sqlite3.connect(DB_PATH)
    sig = read_table(conn, "signals")
    out = read_table(conn, "signal_outcomes")
    if sig.empty or out.empty:
        print("No data: signals or signal_outcomes missing/empty", file=sys.stderr)
        sys.exit(1)

    # normalize columns we rely on
    # signal id
    if "id" in sig.columns:
        sig = sig.rename(columns={"id":"signal_id"})
    # reason/triggers
    if "reason" not in sig.columns:
        # build from triggers if present
        if "triggers" in sig.columns:
            sig["reason"] = sig["triggers"].astype(str)
        else:
            sig["reason"] = ""

    # win column in outcomes
    win_col = "win" if "win" in out.columns else ("is_win" if "is_win" in out.columns else None)
    if not win_col:
        print("No win/is_win column in outcomes", file=sys.stderr)
        sys.exit(1)

    # join
    cols_keep = [c for c in sig.columns if c in ("signal_id","symbol","score","reason","side","venue","interval","ts")]
    s = sig[cols_keep].copy()
    s["bucket"] = s["reason"].astype(str).map(infer_bucket)
    m = out.merge(s, on="signal_id", how="left")

    # score deciles (per bucket)
    m["score"] = pd.to_numeric(m["score"], errors="coerce")
    m["score_decile"] = (
        m.groupby("bucket")["score"]
         .transform(lambda x: pd.qcut(x.rank(method="first"), 10, labels=False, duplicates="drop"))
    )

    # group
    group_cols = ["venue","symbol","bucket","horizon_m","score_decile"]
    agg = (m
           .groupby(group_cols, dropna=False)
           .agg(
               n=("signal_id","nunique"),
               win_rate=(win_col, "mean"),
               mfe=("mfe","mean") if "mfe" in m.columns else (win_col, "count"),
               mae=("mae","mean") if "mae" in m.columns else (win_col, "count"),
               avg_score=("score","mean"),
           )
           .reset_index()
    )

    # pretty print top cuts
    def show(title, df):
        print("\n" + title)
        print(df.to_string(index=False, justify="left", max_colwidth=40))

    # by bucket & horizon (all symbols)
    show("== Win-rate by bucket & horizon ==", 
         agg.groupby(["bucket","horizon_m"])
            .agg(n=("n","sum"), win_rate=("win_rate","mean"))
            .reset_index()
            .sort_values(["bucket","horizon_m"]))

    # top symbols per bucket (horizon 30m as example)
    cut30 = agg[agg["horizon_m"]==30]
    show("== 30m horizon: top symbols by win-rate (min n=30) ==",
         (cut30.groupby(["bucket","symbol"])
               .agg(n=("n","sum"), wr=("win_rate","mean"))
               .reset_index()
               .query("n>=30")
               .sort_values(["bucket","wr"], ascending=[True,False])
               .head(30)))

    # score deciles per bucket
    show("== Win-rate by score decile (all horizons) ==",
         agg.groupby(["bucket","score_decile"])
            .agg(n=("n","sum"), wr=("win_rate","mean"), avg_score=("avg_score","mean"))
            .reset_index()
            .sort_values(["bucket","score_decile"]))

    # also write CSV for your own slicing
    agg.to_csv("agg_bucket_horizon_decile.csv", index=False)
    print("\nWrote: agg_bucket_horizon_decile.csv")

if __name__ == "__main__":
    main()
