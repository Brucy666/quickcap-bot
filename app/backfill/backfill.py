# app/backfill/backfill.py
# Historical backfill: replays bars, logs signals/executions to SQLite,
# and (optionally) mirrors to Supabase when SUPABASE_ENABLED=true.
import asyncio
import argparse
import json
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd

from app.utils import to_dataframe
from app.signals import compute_signals
from app.scoring import score_row
from app.storage.sqlite_store import SQLiteStore
from app.backtest.metrics import compute_outcomes_sqlite
from app.exchanges import KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic
from app.config import load_settings
from app.storage.supabase import Supa  # existing client in your repo

# ---------- Exchange map ----------
SPOT = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}

# ---------- Helpers ----------
async def _fetch_df(ex_cls, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    ex = ex_cls()
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception as e:
        print(f"[ERR] fetch_klines failed {ex_cls.__name__} {symbol} {interval}: {e}")
        return pd.DataFrame()
    return to_dataframe(kl)

def _now_iso_utc(ts_seconds: float) -> str:
    return datetime.utcfromtimestamp(ts_seconds).replace(tzinfo=timezone.utc).isoformat()

# ---------- Core backfill ----------
async def backfill_symbol(
    venue: str,
    symbol: str,
    interval: str,
    lookback: int,
    alert_min_score: float,
    cooldown_sec: int,
    sqlite_path: str,
) -> Dict[str, int]:
    store = SQLiteStore(sqlite_path)

    # Optional Supabase mirror (via env flags)
    cfg = load_settings()
    supa = None
    if getattr(cfg, "supabase_enabled", False) and cfg.supabase_url and cfg.supabase_key:
        try:
            supa = Supa(cfg.supabase_url, cfg.supabase_key)
        except Exception as e:
            print(f"[WARN] Supabase init failed: {e} (continuing with SQLite only)")

    ex_cls = SPOT.get(venue)
    if not ex_cls:
        print(f"[ERR] Unsupported venue: {venue}")
        return {"signals": 0, "executions": 0, "outcomes": 0}

    df = await _fetch_df(ex_cls, symbol, interval, lookback)
    if df is None or len(df) < 100:
        print(f"[WARN] No/insufficient data for {venue}:{symbol}:{interval} (rows={0 if df is None else len(df)})")
        return {"signals": 0, "executions": 0, "outcomes": 0}

    last_alert_ts = 0.0
    sig_ct = exe_ct = 0

    # bar-by-bar replay (no lookahead)
    for i in range(50, len(df) - 1):
        window = df.iloc[: i + 1].copy()
        sig = compute_signals(window)
        last = sig.iloc[-1].copy()
        last["score"] = float(score_row(last))

        # trigger parity with live logic
        triggers: List[str] = []
        if last.get("sweep_long") and last.get("bull_div"):
            triggers.append("VWAP sweep + Bull Div")
        if last.get("sweep_short") and last.get("bear_div"):
            triggers.append("VWAP sweep + Bear Div")
        if bool(last.get("mom_pop")):
            triggers.append("Momentum Pop")

        if not triggers or last["score"] < alert_min_score:
            continue

        now_ts = window["ts"].iloc[-1].timestamp()
        if now_ts - last_alert_ts < cooldown_sec:
            continue
        last_alert_ts = now_ts

        side = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"

        # ---- SIGNAL (SQLite + optional Supabase) ----
        signal_ts_iso = _now_iso_utc(now_ts)
        signal_price = float(last["close"])
        signal_vwap = float(last["vwap"])
        signal_rsi = float(last["rsi"])
        signal_score = float(last["score"])

        signal_id = store.insert_signal(
            {
                "ts": signal_ts_iso,
                "signal_type": "spot",
                "venue": venue,
                "symbol": symbol,
                "interval": interval,
                "side": side,
                "price": signal_price,
                "vwap": signal_vwap,
                "rsi": signal_rsi,
                "score": signal_score,
                "triggers": triggers,
            }
        )
        sig_ct += 1

        if supa:
            # Explicit args to match your Supa client signature
            asyncio.create_task(
                supa.log_signal(
                    ts=signal_ts_iso,
                    signal_type="spot",
                    venue=venue,
                    symbol=symbol,
                    interval=interval,
                    side=side,
                    price=signal_price,
                    vwap=signal_vwap,
                    rsi=signal_rsi,
                    score=signal_score,
                    triggers=triggers,
                )
            )

        # ---- PAPER EXEC @ next bar open ----
        nxt = df.iloc[i + 1]
        exec_ts_iso = _now_iso_utc(nxt["ts"].timestamp())
        exec_price = float(nxt["open"])

        store.insert_execution(
            {
                "ts": exec_ts_iso,
                "venue": "PAPER",
                "symbol": symbol,
                "side": side,
                "price": exec_price,
                "score": signal_score,
                "reason": ", ".join(triggers),
                "is_paper": True,
            }
        )
        exe_ct += 1

        if supa:
            asyncio.create_task(
                supa.log_execution(
                    ts=exec_ts_iso,
                    venue="PAPER",
                    symbol=symbol,
                    side=side,
                    price=exec_price,
                    score=signal_score,
                    reason=", ".join(triggers),
                    is_paper=True,
                )
            )

    # ---- Compute outcomes for horizons (15/30/60m) ----
    out_ct = await compute_outcomes_sqlite(venue, symbol, interval, lookback, store)
    return {"signals": sig_ct, "executions": exe_ct, "outcomes": out_ct}

# ---------- CLI ----------
async def main():
    p = argparse.ArgumentParser(description="QuickCap historical backfill")
    p.add_argument("--venue", default="kucoin", help="kucoin|binance|okx|bybit|mexc")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--lookback", type=int, default=10000, help="number of bars to fetch")
    p.add_argument("--score", type=float, default=2.3, help="min score to log a signal")
    p.add_argument("--cooldown", type=int, default=180, help="seconds between signals per symbol")
    p.add_argument("--sqlite", default="quickcap_results.db")
    args = p.parse_args()

    totals = {"signals": 0, "executions": 0, "outcomes": 0}
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    for sym in symbols:
        res = await backfill_symbol(
            args.venue, sym, args.interval, args.lookback, args.score, args.cooldown, args.sqlite
        )
        for k in totals:
            totals[k] += res[k]
        print(f"[DONE] {args.venue}:{sym}:{args.interval} -> {res}")

    print(json.dumps(totals))

if __name__ == "__main__":
    try:
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
