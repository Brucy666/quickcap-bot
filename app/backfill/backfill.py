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
from app.notifier import NOTIFY   # singleton Discord notifier

# ---- Spot connectors (public) ----
from app.exchanges import (
    KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic
)

SPOT = {
    "kucoin":  KuCoinPublic,
    "mexc":    MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx":     OKXSpotPublic,
    "bybit":   BybitSpotPublic,
}

def _iso_utc(ts: float | int) -> str:
    return datetime.utcfromtimestamp(float(ts)).replace(tzinfo=timezone.utc).isoformat()

async def _fetch_df(ex_cls, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    ex = ex_cls()
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception as e:
        print(f"[BACKFILL] fetch_klines error {ex_cls.__name__} {symbol} {interval}: {e}")
        kl = []
    df = to_dataframe(kl)
    if "close" in df:
        df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
    return df.reset_index(drop=True)

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
    if venue not in SPOT:
        print(f"[BACKFILL] unsupported venue '{venue}'")
        return {"signals": 0, "executions": 0, "outcomes": 0}

    sym = symbol.strip().upper()
    store = SQLiteStore(sqlite_path)

    df = await _fetch_df(SPOT[venue], sym, interval, lookback)
    n = len(df)
    print(f"[BACKFILL] fetched {n} bars for {venue}:{sym}:{interval}")
    if n < 200:
        return {"signals": 0, "executions": 0, "outcomes": 0}

    sig_ct = exe_ct = 0
    last_alert_ts = 0.0

    # walk forward
    warm = max(50, min(200, n // 50))
    for i in range(warm, n - 1):
        window = df.iloc[: i + 1].copy()
        sig = compute_signals(window)
        last = sig.iloc[-1].copy()

        last["score"] = float(score_row(last))
        score = float(last["score"])
        if score < min_score:
            continue

        # triggers (lightweight)
        triggers: List[str] = []
        if last.get("sweep_long"):  triggers.append("VWAP Sweep Long")
        if last.get("sweep_short"): triggers.append("VWAP Sweep Short")
        if last.get("bull_div"):     triggers.append("Bull Div")
        if last.get("bear_div"):     triggers.append("Bear Div")
        if last.get("mom_pop"):      triggers.append("Momentum Pop")
        if not triggers:             continue

        # side
        side = "LONG"
        if last.get("sweep_short") or last.get("bear_div"):
            side = "SHORT"
        if last.get("sweep_long") or last.get("bull_div"):
            side = "LONG"

        now_ts = window["ts"].iloc[-1].timestamp()
        if now_ts - last_alert_ts < cooldown_sec:
            continue
        last_alert_ts = now_ts

        price = float(last.get("close", window["close"].iloc[-1]))
        vwap  = float(last.get("vwap", window.get("vwap", window["close"]).iloc[-1]))
        rsi   = float(last.get("rsi", 0.0))

        # signal row
        sig_row = {
            "ts": _iso_utc(now_ts),
            "signal_type": "spot",
            "venue": venue,
            "symbol": sym,
            "interval": interval,
            "side": side,
            "price": price,
            "vwap": vwap,
            "rsi": rsi,
            "score": score,
            "triggers": triggers,
        }
        store.insert_signal(sig_row); sig_ct += 1
        if supa: asyncio.create_task(supa.log_signal(**sig_row))

        # execution on next bar open (paper)
        nxt = df.iloc[i + 1]
        exec_row = {
            "ts": _iso_utc(nxt["ts"].timestamp()),
            "venue": "PAPER",
            "symbol": sym,
            "side": side,
            "price": float(nxt["open"]),
            "score": score,
            "reason": ", ".join(triggers),
            "is_paper": True,
        }
        store.insert_execution(exec_row); exe_ct += 1
        if supa: asyncio.create_task(supa.log_execution(**exec_row))

    # outcomes computed from *already-fetched* df (no network)
    out_rows = compute_outcomes_from_df(df, venue, sym, interval, store, horizons=(15, 30, 60))
    out_ct = len(out_rows)
    if supa and out_ct:
        # batch insert; if your supabase table has upsert, you can call supa.bulk_insert(...)
        try:
            await supa.bulk_insert("signal_outcomes", out_rows, on_conflict="signal_id,horizon_m")
        except Exception as e:
            print(f"[WARN] supa.bulk_insert(signal_outcomes): {e}")

    # summary → Discord
    try:
        await NOTIFY.backfill_summary(venue, sym, interval, sig_ct, exe_ct, out_ct)
    except Exception as e:
        print(f"[WARN] discord backfill summary: {e}")

    print(f"[BACKFILL] {venue}:{sym}:{interval} -> signals={sig_ct} executions={exe_ct} outcomes={out_ct}")
    return {"signals": sig_ct, "executions": exe_ct, "outcomes": out_ct}

async def _worker(
    venue: str, symbols: List[str], interval: str, lookback: int,
    min_score: float, cooldown_sec: int, sqlite_path: str, supa: Optional[Supa],
    concurrency: int,
) -> Dict[str, int]:
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    totals = {"signals": 0, "executions": 0, "outcomes": 0}

    async def run_one(sym: str):
        async with sem:
            return await backfill_symbol(
                venue=venue, symbol=sym, interval=interval, lookback=lookback,
                min_score=min_score, cooldown_sec=cooldown_sec,
                sqlite_path=sqlite_path, supa=supa,
            )

    results = await asyncio.gather(*(run_one(s) for s in symbols))
    for r in results:
        for k in totals: totals[k] += r.get(k, 0)
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
    p.add_argument("--score", type=float, default=2.8)
    p.add_argument("--cooldown", type=int, default=120)
    p.add_argument("--sqlite", default="quickcap_results.db")
    p.add_argument("--concurrency", type=int, default=4)
    args, _ = p.parse_known_args()

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
        f"interval={args.interval} lookback={args.lookback} score>={args.score} "
        f"cooldown={args.cooldown}s concurrency={args.concurrency}"
    )

    totals = await _worker(
        venue=args.venue, symbols=symbols, interval=args.interval, lookback=args.lookback,
        min_score=args.score, cooldown_sec=args.cooldown, sqlite_path=args.sqlite,
        supa=supa, concurrency=args.concurrency,
    )
    print(json.dumps(totals, ensure_ascii=False))

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
