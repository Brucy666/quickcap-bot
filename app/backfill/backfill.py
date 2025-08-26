# app/backfill/backfill.py
from __future__ import annotations
import os, json, argparse, asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional
import pandas as pd

from app.config import load_settings
from app.utils import to_dataframe
from app.signals import compute_signals
from app.scoring import score_row
from app.storage.sqlite_store import SQLiteStore
from app.storage.supabase import Supa
from app.backtest.metrics import compute_outcomes_from_df
from app.notifier import NOTIFY  # Discord singleton

from app.exchanges import (
    KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic
)

SPOT = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}

def _iso_utc(ts: float | int) -> str:
    return datetime.utcfromtimestamp(float(ts)).replace(tzinfo=timezone.utc).isoformat()

async def _fetch_df(ex_cls, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    ex = ex_cls()
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception as e:
        print(f"[BACKFILL] fetch error {ex_cls.__name__} {symbol}: {e}")
        kl = []
    df = to_dataframe(kl)
    if "close" in df:
        df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
    return df.reset_index(drop=True)

async def backfill_symbol(
    venue: str, symbol: str, interval: str, lookback: int,
    min_score: float, cooldown_sec: int,
    sqlite_path: str, supa: Optional[Supa],
) -> Dict[str, int]:
    sym = symbol.upper()
    df = await _fetch_df(SPOT[venue], sym, interval, lookback)
    if len(df) < 200:
        return {"signals": 0, "executions": 0, "outcomes": 0}

    store = SQLiteStore(sqlite_path)
    sig_ct = exe_ct = 0
    last_alert_ts = 0.0

    for i in range(100, len(df) - 1):
        window = df.iloc[: i + 1].copy()
        sig = compute_signals(window)
        last = sig.iloc[-1].copy()
        score = float(score_row(last))
        last["score"] = score
        if score < min_score:
            continue

        triggers: List[str] = []
        if last.get("sweep_long"):  triggers.append("VWAP Sweep Long")
        if last.get("sweep_short"): triggers.append("VWAP Sweep Short")
        if last.get("bull_div"):    triggers.append("Bull Div")
        if last.get("bear_div"):    triggers.append("Bear Div")
        if last.get("mom_pop"):     triggers.append("Momentum Pop")
        if not triggers:
            continue

        side = "LONG"
        if last.get("sweep_short") or last.get("bear_div"):
            side = "SHORT"

        ts = window["ts"].iloc[-1].timestamp()
        if ts - last_alert_ts < cooldown_sec:
            continue
        last_alert_ts = ts

        sig_row = {
            "ts": _iso_utc(ts), "signal_type": "spot", "venue": venue,
            "symbol": sym, "interval": interval, "side": side,
            "price": float(last["close"]), "vwap": float(last.get("vwap", 0)),
            "rsi": float(last.get("rsi", 0)), "score": score, "triggers": triggers,
        }
        store.insert_signal(sig_row); sig_ct += 1
        if supa: asyncio.create_task(supa.log_signal(**sig_row))

        nxt = df.iloc[i + 1]
        exec_row = {
            "ts": _iso_utc(nxt["ts"].timestamp()), "venue": "PAPER",
            "symbol": sym, "side": side, "price": float(nxt["open"]),
            "score": score, "reason": ", ".join(triggers), "is_paper": True,
        }
        store.insert_execution(exec_row); exe_ct += 1
        if supa: asyncio.create_task(supa.log_execution(**exec_row))

    out_rows = compute_outcomes_from_df(df, venue, sym, interval, store)
    if supa and out_rows:
        await supa.bulk_insert("signal_outcomes", out_rows, on_conflict="signal_id,horizon_m")

    await NOTIFY.backfill_summary(venue, sym, interval, sig_ct, exe_ct, len(out_rows))
    return {"signals": sig_ct, "executions": exe_ct, "outcomes": len(out_rows)}

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--venue", default="binance")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--lookback", type=int, default=1000)
    p.add_argument("--score", type=float, default=2.8)
    p.add_argument("--cooldown", type=int, default=120)
    p.add_argument("--sqlite", default="quickcap_results.db")
    args, _ = p.parse_known_args()

    cfg = load_settings()
    supa = Supa(cfg.supabase_url, cfg.supabase_key) if getattr(cfg, "supabase_enabled", False) else None
    syms = [s.strip().upper() for s in args.symbols.split(",")]

    totals = {"signals": 0, "executions": 0, "outcomes": 0}
    for s in syms:
        res = await backfill_symbol(args.venue, s, args.interval, args.lookback,
                                    args.score, args.cooldown, args.sqlite, supa)
        for k in totals: totals[k] += res[k]

    print(json.dumps(totals, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
