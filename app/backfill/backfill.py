# app/backfill/backfill.py
import asyncio, argparse, json
from datetime import datetime, timezone
import pandas as pd

from app.utils import to_dataframe
from app.signals import compute_signals
from app.scoring import score_row
from app.storage.sqlite_store import SQLiteStore
from app.backtest.metrics import compute_outcomes_sqlite
from app.exchanges import KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic

SPOT = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}

async def fetch_df(ex_cls, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    ex = ex_cls()
    kl = await ex.fetch_klines(symbol, interval, lookback)
    return to_dataframe(kl)

async def backfill_symbol(venue: str, symbol: str, interval: str, lookback: int,
                          alert_min_score: float, cooldown_sec: int, sqlite_path: str):
    store = SQLiteStore(sqlite_path)
    ex_cls = SPOT[venue]
    df = await fetch_df(ex_cls, symbol, interval, lookback)
    if len(df) < 100: 
        return {"signals":0,"executions":0,"outcomes":0}

    last_alert_ts = 0.0
    sig_ct = exe_ct = 0

    # bar-by-bar replay (no lookahead)
    for i in range(50, len(df)-1):
        window = df.iloc[:i+1].copy()
        sig = compute_signals(window)
        last = sig.iloc[-1].copy()
        last["score"] = float(score_row(last))

        triggers = []
        if last.get("sweep_long") and last.get("bull_div"):  triggers.append("VWAP sweep + Bull Div")
        if last.get("sweep_short") and last.get("bear_div"): triggers.append("VWAP sweep + Bear Div")
        if bool(last.get("mom_pop")):                        triggers.append("Momentum Pop")

        if not triggers or last["score"] < alert_min_score:
            continue

        now = window["ts"].iloc[-1].timestamp()
        if now - last_alert_ts < cooldown_sec: 
            continue
        last_alert_ts = now

        side = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"
        ts_iso = datetime.utcfromtimestamp(now).replace(tzinfo=timezone.utc).isoformat()

        signal_id = store.insert_signal({
            "ts": ts_iso,
            "signal_type": "spot",
            "venue": venue,
            "symbol": symbol,
            "interval": interval,
            "side": side,
            "price": float(last["close"]),
            "vwap": float(last["vwap"]),
            "rsi": float(last["rsi"]),
            "score": float(last["score"]),
            "triggers": triggers,
        }); sig_ct += 1

        # deterministic paper fill at next open
        nxt = df.iloc[i+1]
        store.insert_execution({
            "ts": datetime.utcfromtimestamp(nxt["ts"].timestamp()).replace(tzinfo=timezone.utc).isoformat(),
            "venue": "PAPER",
            "symbol": symbol,
            "side": side,
            "price": float(nxt["open"]),
            "score": float(last["score"]),
            "reason": ", ".join(triggers),
            "is_paper": True,
        }); exe_ct += 1

    # compute horizon outcomes for this symbol
    out_ct = await compute_outcomes_sqlite(venue, symbol, interval, lookback, store)
    return {"signals": sig_ct, "executions": exe_ct, "outcomes": out_ct}

async def main():
    p = argparse.ArgumentParser(description="QuickCap historical backfill")
    p.add_argument("--venue", default="kucoin")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--lookback", type=int, default=10000, help="bars to fetch")
    p.add_argument("--score", type=float, default=2.3)
    p.add_argument("--cooldown", type=int, default=180)
    p.add_argument("--sqlite", default="quickcap_results.db")
    args = p.parse_args()

    totals = {"signals":0,"executions":0,"outcomes":0}
    for sym in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        res = await backfill_symbol(args.venue, sym, args.interval, args.lookback,
                                    args.score, args.cooldown, args.sqlite)
        for k in totals: totals[k] += res[k]
    print(json.dumps(totals))

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
