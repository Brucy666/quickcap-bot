# app/main.py
from __future__ import annotations
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
from app.notifier import post_signal_embed
from app.alpha.spot_perp_engine import compute_basis_signals
from app.storage.supabase import Supa
from app.policy import POLICY  # <- central trade filter

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


# ---------- small utils ----------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _build_spot_exchanges(enabled: List[str]):
    return [(name, SPOT_ADAPTERS[name]()) for name in SPOT_ADAPTERS if name in enabled]

def _supa(cfg):
    if getattr(cfg, "supabase_enabled", False) and cfg.supabase_url and cfg.supabase_key:
        try:
            return Supa(cfg.supabase_url, cfg.supabase_key)
        except Exception as e:
            log.warning(f"Supabase init failed: {e}")
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
) -> None:
    if not supa:
        return
    try:
        # fire-and-forget
        asyncio.create_task(supa.log_signal(**signal_payload))
        if exec_payload:
            asyncio.create_task(supa.log_execution(**exec_payload))
    except Exception as e:
        log.error(f"Supabase log error: {e}")


# ---------- core: spot flow ----------

async def _process_symbol(cfg, supa: Supa | None, ex_name: str, ex, symbol: str, executor: PaperExecutor):
    df = await _fetch_symbol(ex, symbol, cfg.interval, cfg.lookback)
    if len(df) < 50 or cfg.risk_off:
        return

    sig = compute_signals(df)
    last = sig.iloc[-1].copy()

    price = float(last.get("close", df["close"].iloc[-1]))
    vwap  = float(last.get("vwap",  df["vwap"].iloc[-1] if "vwap" in df else price))
    rsi   = float(last.get("rsi",   0.0))
    last_score = float(score_row(last))

    # triggers text (for Discord & reason)
    triggers: list[str] = []
    if last.get("sweep_long") and last.get("bull_div"):  triggers.append("VWAP sweep + Bull Div")
    if last.get("sweep_short") and last.get("bear_div"): triggers.append("VWAP sweep + Bear Div")
    if bool(last.get("mom_pop")):                        triggers.append("Momentum Pop")
    reason = ", ".join(triggers)

    # side heuristic
    side = "LONG"
    if last.get("sweep_short") or last.get("bear_div"):
        side = "SHORT"
    if last.get("sweep_long") or last.get("bull_div"):
        side = "LONG"

    # cooldown
    now = time.time()
    key = (ex_name, symbol)
    if now - LAST_ALERT.get(key, 0) < cfg.alert_cooldown_sec:
        return

    # --- POLICY gate (complete context, no 'close' key included)
    dec = POLICY.should_trade(
        symbol=symbol,
        side=side,
        score=last_score,
        reason=reason,
        signal_type="spot",
        rsi=rsi,
        vwap=vwap,
        triggers=triggers,
        venue=ex_name,
        interval=cfg.interval,
        price=price,
    )
    if not dec.take:
        return
    LAST_ALERT[key] = now

    # Supabase row (match schema: no 'close')
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
        "score": float(last_score),
        "triggers": list(triggers),
        "reason": reason,
    }

    # Discord alert
    await post_signal_embed(
        cfg.discord_webhook,
        exchange=ex_name, symbol=symbol, interval=cfg.interval, side=side,
        price=price, vwap=vwap, rsi=rsi, score=float(last_score), triggers=list(triggers),
    )

    # Paper execution
    exec_rec = await executor.submit(symbol, side, price, float(last_score), reason or "signal")
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


# ---------- core: spot–perp basis flow ----------

async def _spot_perp_for_symbol(cfg, supa: Supa | None, venue: str, symbol: str, executor: PaperExecutor):
    pair = PERP_VENUES.get(venue)
    if not pair:
        return
    spot_cls, perp_cls = pair
    spot = spot_cls(); perp = perp_cls()
    s_df = await _fetch_symbol(spot, symbol, cfg.interval, cfg.lookback)
    p_df = await _fetch_symbol(perp, symbol, cfg.interval, cfg.lookback)
    if len(s_df) < 50 or len(p_df) < 50:
        return

    sig = compute_basis_signals(s_df, p_df, z_win=50, z_th=cfg.spot_perp_z)
    if not sig.get("ok") or not sig["triggers"]:
        return

    # basis meta
    basis_pct = float(sig["basis_pct"])
    basis_z   = float(sig["basis_z"])
    side      = sig["side"] or ("SHORT" if basis_pct > 0 else "LONG")
    price     = float(sig["spot_close"])
    vwap      = float(sig["spot_vwap"])
    rsi       = float(sig["spot_rsi"])
    score     = 2.0 + min(abs(basis_z), 5.0)
    triggers  = list(sig["triggers"])
    reason    = "Basis:" + ", ".join(triggers)

    # POLICY gate
    dec = POLICY.should_trade(
        symbol=symbol,
        side=side,
        score=float(score),
        reason=reason,
        signal_type="basis",
        rsi=rsi,
        vwap=vwap,
        triggers=triggers,
        venue=venue,
        interval=cfg.interval,
        price=price,
    )
    if not dec.take:
        return

    # Discord
    await post_signal_embed(
        cfg.discord_webhook,
        exchange=f"{venue}:BASIS", symbol=symbol, interval=cfg.interval, side=side,
        price=price, vwap=vwap, rsi=rsi, score=float(score), triggers=triggers,
        basis_pct=basis_pct, basis_z=basis_z,
    )

    # Supabase signal (NO 'close' field)
    signal_row = {
        "ts": _utc_now_iso(),
        "signal_type": "basis",
        "venue": venue,
        "symbol": symbol,
        "interval": cfg.interval,
        "side": side,
        "price": price,
        "vwap": vwap,
        "rsi": rsi,
        "score": float(score),
        "triggers": triggers,
        "reason": reason,
        # basis extras are supported by supabase.Supa.log_signal via extra kwargs;
        # if your DB doesn't have these cols, Supa will filter them out.
        "basis_pct": basis_pct,
        "basis_z": basis_z,
    }

    # paper exec
    exec_rec = await executor.submit(symbol, side, price, float(score), reason)
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


# ---------- orchestrators ----------

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

    # spot scan
    for ex_name, ex in _build_spot_exchanges(cfg.exchanges):
        for sym in (hotmap.get(ex_name, []) or list(cfg.symbols)):
            await _process_symbol(cfg, supa, ex_name, ex, sym, executor)

    # basis scan
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
        import uvloop  # type: ignore
        uvloop.install()
    except Exception:
        pass
    asyncio.run(main_loop())
