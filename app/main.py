# app/main.py
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import pandas as pd
pd.set_option("future.no_silent_downcasting", True)

import asyncio, time
from datetime import datetime, timezone
from typing import Dict, Tuple, List

from app.config import load_settings
from app.logger import get_logger
from app.utils import to_dataframe
from app.hotlist import build_hotmap
from app.signals import compute_signals
from app.scoring import score_row
from app.executor import PaperExecutor
from app.alpha.spot_perp_engine import compute_basis_signals

from app.storage.supabase import Supa
from app.policy import POLICY                  # central trade filter (cooldowns / thresholds)
from app.notifier import NOTIFY                # Discord notifier singleton

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


def _supa(cfg):
    if getattr(cfg, "supabase_enabled", False) and cfg.supabase_url and cfg.supabase_key:
        return Supa(cfg.supabase_url, cfg.supabase_key)
    return None


async def _fetch_symbol(ex, symbol: str, interval: str, lookback: int):
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
        return to_dataframe(kl)
    except Exception:
        return to_dataframe([])


async def _log_signal_and_exec_to_supa(
    supa: Supa | None,
    signal_payload: dict,
    exec_payload: dict | None = None,
):
    if not supa:
        return
    try:
        asyncio.create_task(supa.log_signal(**signal_payload))
        if exec_payload:
            asyncio.create_task(supa.log_execution(**exec_payload))
    except Exception as e:
        log.error(f"Supabase log error: {e}")


def _mk_reason(triggers: List[str]) -> str:
    return ", ".join(triggers) if triggers else ""


async def _process_symbol(cfg, supa: Supa | None, ex_name: str, ex, symbol: str, executor: PaperExecutor):
    df = await _fetch_symbol(ex, symbol, cfg.interval, cfg.lookback)
    if len(df) < 50 or cfg.risk_off:
        return

    sig = compute_signals(df)
    last = sig.iloc[-1].copy()
    last["score"] = float(score_row(last))

    # Build triggers
    triggers: List[str] = []
    if last.get("sweep_long") and last.get("bull_div"):
        triggers.append("VWAP Sweep Long")
    if last.get("sweep_short") and last.get("bear_div"):
        triggers.append("VWAP Sweep Short")
    if bool(last.get("mom_pop")):
        triggers.append("Momentum Pop")
    if not triggers:
        return

    side = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"
    price = float(last["close"])
    vwap = float(last["vwap"])
    rsi = float(last.get("rsi", 0.0))
    score = float(last["score"])
    reason = _mk_reason(triggers)

    # Policy gate (centralized risk filter) — pass dict (do not unpack)
    ctx = dict(
        signal_type="spot",
        venue=ex_name,
        symbol=symbol,
        interval=cfg.interval,
        side=side,
        score=score,
        reason=reason,
        ts=_utc_now_iso(),
        close=price,
        vwap=vwap,
        rsi=rsi,
        triggers=list(triggers),
    )
    dec = POLICY.should_trade(ctx)
    if not dec.take:
        return

    # Optional local per-symbol cooldown (policy also has its own)
    now = time.time()
    key = (ex_name, symbol)
    if now - LAST_ALERT.get(key, 0) < max(5, int(getattr(cfg, "alert_cooldown_sec", 0) or 0)):
        return
    LAST_ALERT[key] = now

    # Build signal payload (UTC)
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
        "score": score,
        "triggers": triggers,
    }

    # Discord alert
    try:
        await NOTIFY.post_signal_embed(
            exchange=ex_name,
            symbol=symbol,
            interval=cfg.interval,
            side=side,
            price=price,
            vwap=vwap,
            rsi=rsi,
            score=score,
            triggers=triggers,
        )
    except Exception as e:
        log.warning(f"notify post_signal_embed failed: {e}")

    # Paper execution + payload
    exec_rec = await executor.submit(symbol, side, price, score, reason)
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

    # Non-blocking Supabase writes
    await _log_signal_and_exec_to_supa(supa, signal_row, exec_row)


async def _spot_perp_for_symbol(cfg, supa: Supa | None, venue: str, symbol: str, executor: PaperExecutor):
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

    sig = compute_basis_signals(s_df, p_df, z_win=50, z_th=cfg.spot_perp_z)
    if not sig.get("ok") or not sig["triggers"]:
        return

    side = sig["side"] or ("SHORT" if sig["basis_pct"] > 0 else "LONG")
    score = 2.0 + min(abs(sig["basis_z"]), 5.0)
    triggers = list(sig["triggers"])
    reason = "Basis:" + _mk_reason(triggers)

    # Policy gate — pass dict (do not unpack)
    ctx = dict(
        signal_type="basis",
        venue=venue,
        symbol=symbol,
        interval=cfg.interval,
        side=side,
        score=float(score),
        reason=reason,
        ts=_utc_now_iso(),
        close=float(sig["spot_close"]),
        vwap=float(sig["spot_vwap"]),
        rsi=float(sig["spot_rsi"]),
        triggers=triggers,
        basis_pct=float(sig["basis_pct"]),
        basis_z=float(sig["basis_z"]),
    )
    dec = POLICY.should_trade(ctx)
    if not dec.take:
        return

    # Build signal payload
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
        "triggers": triggers,
    }

    # Discord alert (with basis fields)
    try:
        await NOTIFY.post_signal_embed(
            exchange=f"{venue}:BASIS",
            symbol=symbol,
            interval=cfg.interval,
            side=side,
            price=float(sig["spot_close"]),
            vwap=float(sig["spot_vwap"]),
            rsi=float(sig["spot_rsi"]),
            score=float(score),
            triggers=triggers,
            basis_pct=float(sig["basis_pct"]),
            basis_z=float(sig["basis_z"]),
        )
    except Exception as e:
        log.warning(f"notify post_signal_embed (basis) failed: {e}")

    # Paper exec
    exec_rec = await executor.submit(symbol, side, float(sig["spot_close"]), float(score), reason)
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
        hotmap = await build_hotmap(
            cfg.exchanges,
            top_n=cfg.hotlist_top_n,
            min_vol_usdt=cfg.hotlist_min_vol_usdt,
            force_symbols=cfg.force_symbols,
            exclude_symbols=cfg.exclude_symbols,
        )
        log.info("Hotlist per-venue: { " + ", ".join(f"{k}:{len(v)}" for k, v in hotmap.items()) + " }")
    else:
        hotmap = {ex: list(cfg.symbols) for ex in cfg.exchanges}

    for ex_name, ex in _build_spot_exchanges(cfg.exchanges):
        for sym in (hotmap.get(ex_name, []) or list(cfg.symbols)):
            await _process_symbol(cfg, supa, ex_name, ex, sym, executor)

    if cfg.spot_perp_enabled:
        for venue in cfg.spot_perp_exchanges:
            for sym in (hotmap.get(venue, []) or list(cfg.symbols)):
                await _spot_perp_for_symbol(cfg, supa, venue, sym, executor)


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
