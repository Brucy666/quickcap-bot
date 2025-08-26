# app/backfill/backfill.py
from __future__ import annotations

import asyncio
import argparse
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from app.config import load_settings
from app.utils import to_dataframe
from app.signals import compute_signals
from app.scoring import score_row
from app.storage.sqlite_store import SQLiteStore
from app.backtest.metrics import compute_outcomes_sqlite_rows
from app.storage.supabase import Supa
from app.notifier import post_backfill_summary

# ---- Spot connectors (public) ----
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


# -------------------- helpers --------------------

def _iso_utc(ts: float | int) -> str:
    return datetime.utcfromtimestamp(float(ts)).replace(tzinfo=timezone.utc).isoformat()

async def _fetch_df(ex_cls, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    """
    Fetch klines and normalize to a dataframe with at least: ts, open, high, low, close, vwap.
    """
    ex = ex_cls()
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception as e:
        print(f"[BACKFILL] fetch_klines error {ex_cls.__name__} {symbol} {interval}: {e}")
        kl = []
    df = to_dataframe(kl)
    # sanity: drop obvious bad rows
    if "close" in df:
        df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
    return df.reset_index(drop=True)


# -------------------- core backfill --------------------

async def backfill_symbol(
    venue: str,
    symbol: str,
    interval: str,
    lookback: int,
    min_score: float,
    cooldown_sec: int,
    sqlite_path: str,
    supa: Optional[Supa],
) -> Dict[str, int]:
    """
    Replays signals on historical bars (no lookahead) and logs signals/executions/outcomes.
    Returns counts for summary.
    """
    if venue not in SPOT:
        print(f"[BACKFILL] unsupported venue '{venue}'")
        return {"signals": 0, "executions": 0, "outcomes": 0}

    norm_symbol = symbol.strip().upper()
    store = SQLiteStore(sqlite_path)

    df = await _fetch_df(SPOT[venue], norm_symbol, interval, lookback)
    n = len(df)
    print(f"[BACKFILL] fetched {n} bars for {venue}:{norm_symbol}:{interval}")
    if n < 200:
        return {"signals": 0, "executions": 0, "outcomes": 0}

    last_alert_ts = 0.0
    sig_ct = exe_ct = 0

    # replay through time (use up to bar i to compute signal for bar i)
    # start with a buffer so indicators warm up
    start_i = max(50, min(200, n // 50))
    for i in range(start_i, n - 1):
        window = df.iloc[: i + 1].copy()
        sig = compute_signals(window)
        last = sig.iloc[-1].copy()

        # score with your improved function
        last_score = float(score_row(last))
        last["score"] = last_score

        # Build triggers list for display/audit
        triggers: List[str] = []
        if last.get("sweep_long"):   triggers.append("VWAP Sweep Long")
        if last.get("sweep_short"):  triggers.append("VWAP Sweep Short")
        if last.get("bull_div"):     triggers.append("Bull Div")
        if last.get("bear_div"):     triggers.append("Bear Div")
        if last.get("mom_pop"):      triggers.append("Momentum Pop")

        # Quality gates
        if not triggers:
            continue
        if last_score < min_score:
            continue

        # simple side heuristic: prefer sweep/div; else infer from divergences
        side = "LONG"
        if last.get("sweep_short") or last.get("bear_div"):
            side = "SHORT"
        if last.get("sweep_long") or last.get("bull_div"):
            side = "LONG"

        # cooldown to avoid signal spam
        now_ts = window["ts"].iloc[-1].timestamp() if hasattr(window["ts"].iloc[-1], "timestamp") else float(window["ts"].iloc[-1])
        if now_ts - last_alert_ts < cooldown_sec:
            continue
        last_alert_ts = now_ts

        # ---- log signal
        sig_row = {
            "ts": _iso_utc(now_ts),
            "signal_type": "spot",
            "venue": venue,
            "symbol": norm_symbol,
            "interval": interval,
            "side": side,
            "price": float(last.get("close", window["close"].iloc[-1])),
            "vwap": float(last.get("vwap", window.get("vwap", window["close"]).iloc[-1])),
            "rsi": float(last.get("rsi", 0.0)),
            "score": last_score,
            "triggers": triggers,
        }
        store.insert_signal(sig_row)
        sig_ct += 1
        if supa:
            asyncio.create_task(supa.log_signal(**sig_row))

        # ---- naive execution on next bar open (paper)
        nxt = df.iloc[i + 1]
        nxt_ts = nxt["ts"].timestamp() if hasattr(nxt["ts"], "timestamp") else float(nxt["ts"])
        exec_row = {
            "ts": _iso_utc(nxt_ts),
            "venue": "PAPER",
            "symbol": norm_symbol,
            "side": side,
            "price": float(nxt["open"]),
            "score": last_score,
            "reason": ", ".join(triggers),
            "is_paper": True,
        }
        store.insert_execution(exec_row)
        exe_ct += 1
        if supa:
            asyncio.create_task(supa.log_execution(**exec_row))

    # ---- outcomes
    out_rows = await compute_outcomes_sqlite_rows(venue, norm_symbol, interval, lookback, store)
    out_ct = len(out_rows)
    if supa and out_ct:
        try:
            await supa.bulk_insert("signal_outcomes", out_rows, on_conflict="signal_id,horizon_m")
        except Exception as e:
            print(f"[WARN] supa.bulk_insert(signal_outcomes): {e}")

    print(f"[BACKFILL] {venue}:{norm_symbol}:{interval} -> signals={sig_ct} executions={exe_ct} outcomes={out_ct}")

    # Discord summary (best-effort)
    try:
        await post_backfill_summary(venue, norm_symbol, interval, sig_ct, exe_ct, out_ct)
    except Exception as e:
        print(f"[WARN] backfill summary discord: {e}")

    return {"signals": sig_ct, "executions": exe_ct, "outcomes": out_ct}


# -------------------- CLI runner --------------------

async def _worker(
    venue: str,
    symbols: List[str],
    interval: str,
    lookback: int,
    min_score: float,
    cooldown_sec: int,
    sqlite_path: str,
    supa: Optional[Supa],
    concurrency: int,
) -> Dict[str, int]:
    sem = asyncio.Semaphore(concurrency)
    totals = {"signals": 0, "executions": 0, "outcomes": 0}

    async def run_one(sym: str):
        async with sem:
            return await backfill_symbol(
                venue=venue,
                symbol=sym,
                interval=interval,
                lookback=lookback,
                min_score=min_score,
                cooldown_sec=cooldown_sec,
                sqlite_path=sqlite_path,
                supa=supa,
            )

    results = await asyncio.gather(*(run_one(s) for s in symbols))
    for r in results:
        for k in totals:
            totals[k] += r.get(k, 0)
    return totals


async def main():
    p = argparse.ArgumentParser(
        description="QuickCap historical backfill (spot)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--venue", default="binance", help="kucoin|binance|okx|bybit|mexc")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--lookback", type=int, default=1000)
    p.add_argument("--score", type=float, default=2.8, help="minimum score gate")
    p.add_argument("--cooldown", type=int, default=120, help="min seconds between alerts per symbol")
    p.add_argument("--sqlite", default="quickcap_results.db")
    p.add_argument("--concurrency", type=int, default=4)

    # Be tolerant of unknown flags (so old scripts like --basis-z-th won't crash)
    args, _unknown = p.parse_known_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    cfg = load_settings()
    supa = None
    try:
        if getattr(cfg, "supabase_enabled", False):
            supa = Supa(cfg.supabase_url, cfg.supabase_key)
    except Exception as e:
        print(f"[WARN] Supabase init: {e}")

    print(
        f"[BACKFILL] starting :: venue={args.venue} symbols={len(symbols)} "
        f"interval={args.interval} lookback={args.lookback} "
        f"score>={args.score} cooldown={args.cooldown}s concurrency={args.concurrency}"
    )

    totals = await _worker(
        venue=args.venue,
        symbols=symbols,
        interval=args.interval,
        lookback=args.lookback,
        min_score=args.score,
        cooldown_sec=args.cooldown,
        sqlite_path=args.sqlite,
        supa=supa,
        concurrency=args.concurrency,
    )

    print(json.dumps(totals, ensure_ascii=False))

    # (Optional) trigger performance report step here if you have a runner script
    # You can leave this out if you prefer running performance separately.


if __name__ == "__main__":
    try:
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
