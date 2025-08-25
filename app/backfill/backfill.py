# app/backfill/backfill.py
import asyncio, argparse, json
from datetime import datetime, timezone
from typing import List

import pandas as pd

from app.utils import to_dataframe
from app.signals import compute_signals
from app.scoring import score_row
from app.storage.sqlite_store import SQLiteStore
from app.backtest.metrics import compute_outcomes_sqlite_rows
from app.exchanges import KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic
from app.config import load_settings
from app.storage.supabase import Supa

SPOT = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}

def _iso_utc(ts: float) -> str:
    return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc).isoformat()

async def _fetch_df(ex_cls, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    ex = ex_cls()
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception:
        kl = []
    return to_dataframe(kl)

async def backfill_symbol(venue: str, symbol: str, interval: str, lookback: int,
                          alert_min_score: float, cooldown_sec: int, sqlite_path: str,
                          supa: Supa | None):
    store = SQLiteStore(sqlite_path)
    df = await _fetch_df(SPOT[venue], symbol, interval, lookback)
    if len(df) < 100:
        return {"signals":0,"executions":0,"outcomes":0}

    last_alert = 0.0
    sig_ct = exe_ct = 0
    for i in range(50, len(df)-1):
        window = df.iloc[:i+1].copy()
        sig = compute_signals(window)
        last = sig.iloc[-1].copy()
        last["score"] = float(score_row(last))

        triggers: List[str] = []
        if last.get("sweep_long") and last.get("bull_div"):  triggers.append("VWAP sweep + Bull Div")
        if last.get("sweep_short") and last.get("bear_div"): triggers.append("VWAP sweep + Bear Div")
        if bool(last.get("mom_pop")):                        triggers.append("Momentum Pop")
        if not triggers or last["score"] < alert_min_score:
            continue

        now = window["ts"].iloc[-1].timestamp()
        if now - last_alert < cooldown_sec:
            continue
        last_alert = now

        side = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"
        sig_ts = _iso_utc(now)

        signal_row = {
            "ts": sig_ts,
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
        }
        store.insert_signal(signal_row); sig_ct += 1
        if supa:
            asyncio.create_task(supa.log_signal(**signal_row))

        nxt = df.iloc[i+1]
        exec_row = {
            "ts": _iso_utc(nxt["ts"].timestamp()),
            "venue": "PAPER",
            "symbol": symbol,
            "side": side,
            "price": float(nxt["open"]),
            "score": float(last["score"]),
            "reason": ", ".join(triggers),
            "is_paper": True,
        }
        store.insert_execution(exec_row); exe_ct += 1
        if supa:
            asyncio.create_task(supa.log_execution(**exec_row))

    # compute outcomes (writes to SQLite) and mirror to Supabase
    out_rows = await compute_outcomes_sqlite_rows(venue, symbol, interval, lookback, store)
    if supa and out_rows:
        # idempotent via (signal_id,horizon_m) unique key in schema
        await supa.bulk_insert("signal_outcomes", out_rows, on_conflict="signal_id,horizon_m")

    return {"signals":sig_ct,"executions":exe_ct,"outcomes":len(out_rows)}

async def main():
    p = argparse.ArgumentParser(description="QuickCap historical backfill")
    p.add_argument("--venue", default="kucoin")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--lookback", type=int, default=50000)
    p.add_argument("--score", type=float, default=2.3)
    p.add_argument("--cooldown", type=int, default=180)
    p.add_argument("--sqlite", default="quickcap_results.db")
    args = p.parse_args()

    cfg = load_settings()
    supa = Supa(cfg.supabase_url, cfg.supabase_key) if getattr(cfg, "supabase_enabled", False) else None

    totals = {"signals":0,"executions":0,"outcomes":0}
    for sym in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        res = await backfill_symbol(args.venue, sym, args.interval, args.lookback,
                                    args.score, args.cooldown, args.sqlite, supa)
        for k in totals: totals[k] += res[k]
        print(f"[BACKFILL] {args.venue}:{sym}:{args.interval} -> {res}")

    print(json.dumps(totals))

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
