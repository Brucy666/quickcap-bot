# app/main.py
from __future__ import annotations
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import pandas as pd
pd.set_option("future.no_silent_downcasting", True)

import asyncio, time
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Optional

from app.config import load_settings
from app.logger import get_logger
from app.utils import to_dataframe
from app.hotlist import build_hotmap
from app.signals import compute_signals
from app.scoring import score_row
from app.executor import PaperExecutor
from app.notifier import post_signal_embed
from app.alpha.spot_perp_engine import compute_basis_signals
from app.storage.supabase import Supa
from app.policy import POLICY  # ← central trade filter

from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic,
    BinancePerpPublic, OKXPerpPublic, BybitPerpPublic,
)

log = get_logger("main")
LAST_ALERT: Dict[Tuple[str, str], float] = {}

SPOT_ADAPTERS = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}
PERP_VENUES = {
    "binance": (BinanceSpotPublic, BinancePerpPublic),
    "okx": (OKXSpotPublic, OKXPerpPublic),
    "bybit": (BybitSpotPublic, BybitPerpPublic),
}

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _build_spot_exchanges(enabled: List[str]):
    return [(name, SPOT_ADAPTERS[name]()) for name in SPOT_ADAPTERS if name in enabled]

def _supa(cfg) -> Optional[Supa]:
    try:
        if getattr(cfg, "supabase_enabled", False) and cfg.supabase_url and cfg.supabase_key:
            return Supa(cfg.supabase_url, cfg.supabase_key)
    except Exception as e:
        log.warning(f"Supabase init skipped: {e}")
    return None

async def _fetch_symbol(ex, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
        df = to_dataframe(kl)
        if "close" in df:
            df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
        return df.reset_index(drop=True)
    except Exception as e:
        log.error(f"fetch_klines failed {getattr(ex, '__class__', type(ex)).__name__}:{symbol}:{interval} -> {e}")
        return to_dataframe([])

async def _log_signal_and_exec_to_supa(
    supa: Optional[Supa],
    signal_payload: dict,
    exec_payload: Optional[dict] = None,
):
    if not supa:
        return
    try:
        asyncio.create_task(supa.log_signal(**signal_payload))
        if exec_payload:
            asyncio.create_task(supa.log_execution(**exec_payload))
    except Exception as e:
        log.error(f"Supabase log error: {e}")

def _mk_triggers(last: pd.Series) -> List[str]:
    t: List[str] = []
    if last.get("sweep_long") and last.get("bull_div"):  t.append("VWAP sweep + Bull Div")
    if last.get("sweep_short") and last.get("bear_div"): t.append("VWAP sweep + Bear Div")
    if bool(last.get("mom_pop")):                        t.append("Momentum Pop")
    return t

async def _process_symbol(cfg, supa: Optional[Supa], ex_name: str, ex, symbol: str, executor: PaperExecutor):
    df = await _fetch_symbol(ex, symbol, cfg.interval, cfg.lookback)
    if len(df) < 50 or cfg.risk_off:
        return

    sig = compute_signals(df)
    last = sig.iloc[-1].copy()

    # score with your improved function
    last_score = float(score_row(last))
    last["score"] = last_score

    triggers = _mk_triggers(last)
    if not triggers or last_score < float(cfg.alert_min_score):
        return

    now = time.time()
    key = (ex_name, symbol)
    if now - LAST_ALERT.get(key, 0.0) < float(cfg.alert_cooldown_sec):
        return
    LAST_ALERT[key] = now

    # side heuristic (div/sweep have priority)
    side = "LONG"
    if last.get("sweep_short") or last.get("bear_div"):
        side = "SHORT"
    if last.get("sweep_long") or last.get("bull_div"):
        side = "LONG"

    # Build signal row (complete dict for POLICY + logging)
    price = float(last.get("close", df["close"].iloc[-1]))
    vwap  = float(last.get("vwap",  df.get("vwap", df["close"]).iloc[-1]))
    rsi   = float(last.get("rsi",   50.0))

    signal_row = {
        "ts": _utc_now_iso(),
        "signal_type": "spot",
        "venue": ex_name,
        "symbol": symbol,
        "interval": cfg.interval,
        "side": side,
        "price": price,
        "vwap": vwap,
        "rsi": rsi,
        "score": last_score,
        "triggers": triggers,
    }

    # ---- Policy gate (single source of truth)
    dec = POLICY.should_trade(signal_row)
    if not dec.take:
        log.info(f"[SKIP] {ex_name}:{symbol} {side} score={last_score:.3f} :: {dec.why}")
        return

    # ---- Discord alert
    await post_signal_embed(
        cfg.discord_webhook,
        exchange=ex_name, symbol=symbol, interval=cfg.interval, side=side,
        price=price, vwap=vwap, rsi=rsi, score=last_score, triggers=triggers,
    )

    # ---- Paper execution
    exec_rec = await executor.submit(symbol, side, price, last_score, ", ".join(triggers))
    exec_row = {
        "ts": datetime.fromtimestamp(exec_rec["ts"], tz=timezone.utc).isoformat(),
        "venue": exec_rec["venue"],
        "symbol": exec_rec["symbol"],
        "side": exec_rec["side"],
        "price": exec_rec["price"],
        "score": exec_rec["score"],
        "reason": exec_rec["reason"],
        "is_paper": exec_rec["is_paper"],
    }

    await _log_signal_and_exec_to_supa(supa, signal_row, exec_row)

async def _spot_perp_for_symbol(cfg, supa: Optional[Supa], venue: str, symbol: str, executor: PaperExecutor):
    pair = PERP_VENUES.get(venue)
    if not pair:
        return

    spot_cls, perp_cls = pair
    spot = spot_cls()
    perp = perp_cls()
    s_df = await _fetch_symbol(spot, symbol, cfg.interval, cfg.lookback)
    p_df = await _fetch_symbol(perp, symbol, cfg.interval, cfg.lookback)
    if len(s_df) < 50 or len(p_df) < 50:
        return

    sig = compute_basis_signals(s_df, p_df, z_win=50, z_th=float(cfg.spot_perp_z))
    if not sig.get("ok") or not sig["triggers"]:
        return

    side  = sig["side"] or ("SHORT" if sig["basis_pct"] > 0 else "LONG")
    score = 2.0 + min(abs(float(sig["basis_z"])), 5.0)

    # Build a complete signal row (so POLICY + downstream can use it consistently)
    signal_row = {
        "ts": _utc_now_iso(),
        "signal_type": "basis",
        "venue": venue,
        "symbol": symbol,
        "interval": cfg.interval,
        "side": side,
        "price": float(sig["spot_close"]),
        "vwap": float(sig["spot_vwap"]),
        "rsi": float(sig["spot_rsi"]),
        "score": float(score),
        "triggers": list(sig["triggers"]),
    }

    # ---- Policy gate
    dec = POLICY.should_trade(signal_row)
    if not dec.take:
        log.info(f"[SKIP] {venue}:BASIS {symbol} {side} score={score:.3f} :: {dec.why}")
        return

    # ---- Discord
    await post_signal_embed(
        cfg.discord_webhook,
        exchange=f"{venue}:BASIS", symbol=symbol, interval=cfg.interval, side=side,
        price=float(sig["spot_close"]), vwap=float(sig["spot_vwap"]), rsi=float(sig["spot_rsi"]),
        score=float(score), triggers=list(sig["triggers"]),
        basis_pct=float(sig["basis_pct"]), basis_z=float(sig["basis_z"]),
    )

    # ---- Paper exec
    exec_rec = await executor.submit(symbol, side, float(sig["spot_close"]), float(score), "Basis:" + ",".join(sig["triggers"]))
    exec_row = {
        "ts": datetime.fromtimestamp(exec_rec["ts"], tz=timezone.utc).isoformat(),
        "venue": exec_rec["venue"],
        "symbol": exec_rec["symbol"],
        "side": exec_rec["side"],
        "price": exec_rec["price"],
        "score": exec_rec["score"],
        "reason": exec_rec["reason"],
        "is_paper": exec_rec["is_paper"],
    }

    await _log_signal_and_exec_to_supa(supa, signal_row, exec_row)

async def scan_once():
    cfg = load_settings()
    supa = _supa(cfg)
    executor = PaperExecutor(cfg.max_pos_usdt)

    if cfg.hotlist_enabled:
        hotmap = await build_hotlist(cfg)  # small wrapper to keep the signature tidy
    else:
        hotmap = {ex: list(cfg.symbols) for ex in cfg.exchanges}

    # spot venues
    for ex_name, ex in _build_spot_exchanges(cfg.exchanges):
        for sym in (hotmap.get(ex_name, []) or list(cfg.symbols)):
            await _process_symbol(cfg, supa, ex_name, ex, sym, executor)

    # basis (spot-perp)
    if cfg.spot_perp_enabled:
        for venue in cfg.spot_perp_exchanges:
            for sym in (hotlist_symbols := (hotmap.get(venue, []) or list(cfg.symbols))):
                await _spot_perp_for_symbol(cfg, supa, venue, sym, executor)

async def build_hotlist(cfg):
    hotmap = await build_hotmap(
        cfg.exchanges,
        top_n=cfg.hotlist_top_n,
        min_vol_usdt=cfg.hotlist_min_vol_usdt,
        force_symbols=cfg.force_symbols,
        exclude_symbols=cfg.exclude_symbols,
    )
    log.info("Hotlist per-venue: { " + ", ".join(f"{k}:{len(v)}" for k, v in hotmap.items()) + " }")
    return hotmap

async def main_loop():
    cfg = load_settings()
    period = max(10, int(cfg.scan_period_sec))
    log.info(f"Starting scan loop | exchanges={cfg.exchanges} interval={cfg.interval} period={period}s")
    while True:
        await scan_once()
        await asyncio.sleep(period)

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main_loop())
