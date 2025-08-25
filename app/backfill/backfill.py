# app/backfill/backfill.py
from __future__ import annotations

import asyncio, argparse, json, os
from datetime import datetime, timezone
from typing import Dict, List
import pandas as pd

from app.config import load_settings
from app.utils import to_dataframe
from app.signals import compute_signals
from app.scoring import score_row
from app.storage.sqlite_store import SQLiteStore
from app.backtest.metrics import compute_outcomes_sqlite_rows
from app.storage.supabase import Supa
from app.notifier import post_backfill_summary
from app.alpha.spot_perp_engine import compute_basis_signals

from app.exchanges import (
    KuCoinPublic, MEXCPublic, BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic,
    BinancePerpPublic, OKXPerpPublic, BybitPerpPublic,
)

SPOT = {
    "kucoin": KuCoinPublic, "mexc": MEXCPublic, "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic, "bybit": BybitSpotPublic,
}
PERP = {
    "binance": BinancePerpPublic, "okx": OKXPerpPublic, "bybit": BybitPerpPublic,
}

# --------------------- util ---------------------
def _iso_utc(ts: float) -> str:
    return datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc).isoformat()

async def _fetch_df(ex_cls, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    ex = ex_cls()
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception as e:
        print(f"[BACKFILL] fetch_klines error {ex_cls.__name__} {symbol} {interval}: {e}")
        kl = []
    return to_dataframe(kl)

# --------------------- SPOT mode ---------------------
async def backfill_symbol_spot(
    venue: str, symbol: str, interval: str, lookback: int,
    min_score: float, cooldown_sec: int, sqlite_path: str, supa: Supa | None,
) -> Dict[str, int]:
    store = SQLiteStore(sqlite_path)
    df = await _fetch_df(SPOT[venue], symbol, interval, lookback)
    print(f"[BACKFILL] fetched {len(df)} bars for {venue}:{symbol}:{interval}")
    if len(df) < 100: return {"signals":0,"executions":0,"outcomes":0}

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
        if not triggers or last["score"] < min_score:        continue

        now = window["ts"].iloc[-1].timestamp()
        if now - last_alert < cooldown_sec:                  continue
        last_alert = now

        side = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"
        sig_row = {
            "ts": _iso_utc(now), "signal_type":"spot", "venue":venue, "symbol":symbol,
            "interval":interval, "side":side, "price":float(last["close"]), "vwap":float(last["vwap"]),
            "rsi":float(last["rsi"]), "score":float(last["score"]), "triggers":triggers,
        }
        store.insert_signal(sig_row); sig_ct += 1
        if supa: asyncio.create_task(supa.log_signal(**sig_row))

        nxt = df.iloc[i+1]
        exec_row = {
            "ts": _iso_utc(nxt["ts"].timestamp()), "venue":"PAPER", "symbol":symbol,
            "side":side, "price":float(nxt["open"]), "score":float(last["score"]),
            "reason":", ".join(triggers), "is_paper": True,
        }
        store.insert_execution(exec_row); exe_ct += 1
        if supa: asyncio.create_task(supa.log_execution(**exec_row))

    out_rows = await compute_outcomes_sqlite_rows(venue, symbol, interval, lookback, store)
    if supa and out_rows:
        try: await supa.bulk_insert("signal_outcomes", out_rows, on_conflict="signal_id,horizon_m")
        except Exception as e: print(f"[WARN] supa.bulk_insert(signal_outcomes): {e}")

    print(f"[BACKFILL] {venue}:{symbol}:{interval} (spot) -> signals={sig_ct} executions={exe_ct} outcomes={len(out_rows)}")
    try:    await post_backfill_summary(venue, symbol, interval, sig_ct, exe_ct, len(out_rows))
    except Exception as e: print(f"[WARN] backfill summary discord: {e}")
    return {"signals":sig_ct,"executions":exe_ct,"outcomes":len(out_rows)}

# --------------------- BASIS mode ---------------------
async def backfill_symbol_basis(
    venue: str, symbol: str, interval: str, lookback: int,
    min_score: float, cooldown_sec: int, sqlite_path: str, supa: Supa | None,
    z_win: int, z_th: float,
) -> Dict[str, int]:
    if venue not in PERP:
        print(f"[BACKFILL] basis mode unsupported for venue '{venue}'")
        return {"signals":0,"executions":0,"outcomes":0}

    store = SQLiteStore(sqlite_path)
    spot_df = await _fetch_df(SPOT[venue], symbol, interval, lookback)
    perp_df = await _fetch_df(PERP[venue], symbol, interval, lookback)
    print(f"[BACKFILL] fetched spot={len(spot_df)} perp={len(perp_df)} for {venue}:{symbol}:{interval}")
    if len(spot_df) < 100 or len(perp_df) < 100: 
        return {"signals":0,"executions":0,"outcomes":0}

    last_alert = 0.0
    sig_ct = exe_ct = 0
    length = min(len(spot_df), len(perp_df))
    for i in range(max(50, z_win), length-1):
        s_win = spot_df.iloc[:i+1].copy()
        p_win = perp_df.iloc[:i+1].copy()

        res = compute_basis_signals(s_win, p_win, z_win=z_win, z_th=z_th)
        if not res.get("ok") or not res["triggers"]:
            continue

        side = res["side"] or ("SHORT" if float(res["basis_pct"]) > 0 else "LONG")
        score = 2.0 + min(abs(float(res["basis_z"])), 5.0)
        if score < min_score: 
            continue

        now = s_win["ts"].iloc[-1].timestamp()
        if now - last_alert < cooldown_sec:
            continue
        last_alert = now

        sig_row = {
            "ts": _iso_utc(now), "signal_type":"basis", "venue":venue, "symbol":symbol,
            "interval":interval, "side":side,
            "price": float(res["spot_close"]), "vwap": float(res["spot_vwap"]), "rsi": float(res["spot_rsi"]),
            "score": float(score), "triggers": list(res["triggers"]),
        }
        store.insert_signal(sig_row); sig_ct += 1
        if supa: asyncio.create_task(supa.log_signal(**sig_row))

        # exec at next bar open on SPOT feed
        nxt = spot_df.iloc[i+1]
        exec_row = {
            "ts": _iso_utc(nxt["ts"].timestamp()), "venue":"PAPER", "symbol":symbol,
            "side":side, "price":float(nxt["open"]), "score":float(score),
            "reason":"Basis:" + ",".join(res["triggers"]), "is_paper": True,
        }
        store.insert_execution(exec_row); exe_ct += 1
        if supa: asyncio.create_task(supa.log_execution(**exec_row))

    out_rows = await compute_outcomes_sqlite_rows(venue, symbol, interval, lookback, store)
    if supa and out_rows:
        try: await supa.bulk_insert("signal_outcomes", out_rows, on_conflict="signal_id,horizon_m")
        except Exception as e: print(f"[WARN] supa.bulk_insert(signal_outcomes): {e}")

    print(f"[BACKFILL] {venue}:{symbol}:{interval} (basis) -> signals={sig_ct} executions={exe_ct} outcomes={len(out_rows)}")
    try:    await post_backfill_summary(venue, symbol, interval, sig_ct, exe_ct, len(out_rows))
    except Exception as e: print(f"[WARN] backfill summary discord: {e}")
    return {"signals":sig_ct,"executions":exe_ct,"outcomes":len(out_rows)}

# --------------------- CLI ---------------------
async def main():
    p = argparse.ArgumentParser(description="QuickCap historical backfill")
    # Safe defaults: BASIS mode on Binance, 1m, 2 weeks data, relaxed gates
    p.add_argument("--mode", choices=["spot","basis"], default="basis")
    p.add_argument("--venue", default="binance")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT,DOGEUSDT,LTCUSDT,XRPUSDT,SUIUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--lookback", type=int, default=20000)
    p.add_argument("--score", type=float, default=1.5)
    p.add_argument("--cooldown", type=int, default=60)
    p.add_argument("--sqlite", default="quickcap_results.db")
    # basis tuning
    p.add_argument("--basis-z-win", type=int, default=50)
    p.add_argument("--basis-z-th", type=float, default=1.0)
    args = p.parse_args()

    cfg = load_settings()
    supa = Supa(cfg.supabase_url, cfg.supabase_key) if getattr(cfg,"supabase_enabled",False) else None

    totals = {"signals":0,"executions":0,"outcomes":0}
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    for sym in symbols:
        if args.mode == "spot":
            res = await backfill_symbol_spot(args.venue, sym, args.interval, args.lookback,
                                             args.score, args.cooldown, args.sqlite, supa)
        else:
            res = await backfill_symbol_basis(args.venue, sym, args.interval, args.lookback,
                                              args.score, args.cooldown, args.sqlite, supa,
                                              args.basis_z_win, args.basis_z_th)
        for k in totals: totals[k] += res[k]
        print(f"[BACKFILL] {args.venue}:{sym}:{args.interval} -> {res}")

    print(json.dumps(totals))

    # Performance report
    try:
        os.environ.setdefault("SUPABASE_URL", cfg.supabase_url)
        os.environ.setdefault("SUPABASE_KEY", cfg.supabase_key)
        import subprocess, sys
        print("[BACKFILL] running performance report...")
        subprocess.run([sys.executable, "-m", "app.tools.report_to_discord"], check=True)
    except Exception as e:
        print(f"[BACKFILL] report step failed: {e}")

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
