# app/main.py
from __future__ import annotations

# ---- housekeeping / pandas warnings -----------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import pandas as pd
pd.set_option("future.no_silent_downcasting", True)

# ---- stdlib ------------------------------------------------------------------
import asyncio, time
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Optional

# ---- project imports ---------------------------------------------------------
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
from app.policy import POLICY  # TradingPolicy singleton

from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic,
    BinancePerpPublic, OKXPerpPublic, BybitPerpPublic,
)

log = get_logger("main")

# ------------------------------------------------------------------------------
# EXCHANGE REGISTRY
# ------------------------------------------------------------------------------
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

LAST_ALERT: Dict[Tuple[str, str], float] = {}  # (venue,symbol) -> last ts

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ------------------------------------------------------------------------------
# SINGLETON SUPABASE CLIENT
# ------------------------------------------------------------------------------
_SUPA_SINGLETON: Optional[Supa] = None

def get_supa(cfg) -> Optional[Supa]:
    """Create once and reuse the same Supabase client."""
    global _SUPA_SINGLETON
    if not getattr(cfg, "supabase_enabled", False):
        return None
    if _SUPA_SINGLETON is None:
        _SUPA_SINGLETON = Supa(cfg.supabase_url, cfg.supabase_key)
    return _SUPA_SINGLETON

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------
async def _fetch_symbol(ex, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception as e:
        log.warning(f"fetch_klines failed {getattr(ex,'__class__',type(ex)).__name__} {symbol}: {e}")
        kl = []
    df = to_dataframe(kl)
    if "close" in df:
        df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
    return df.reset_index(drop=True)

async def _log_to_supa(
    supa: Optional[Supa],
    signal_row: Optional[dict] = None,
    exec_row: Optional[dict] = None,
):
    if not supa:
        return
    try:
        if signal_row:
            asyncio.create_task(supa.log_signal(**signal_row))
        if exec_row:
            asyncio.create_task(supa.log_execution(**exec_row))
    except Exception as e:
        log.error(f"Supabase log error: {e}")

# ------------------------------------------------------------------------------
# SPOT SIGNALS
# ------------------------------------------------------------------------------
async def _process_symbol(cfg, supa: Optional[Supa], venue: str, ex, symbol: str, executor: PaperExecutor):
    df = await _fetch_symbol(ex, symbol, cfg.interval, cfg.lookback)
    if len(df) < 50 or cfg.risk_off:
        return

    sig = compute_signals(df)
    row = sig.iloc[-1].copy()
    row["score"] = float(score_row(row))

    # triggers for embed / reason text
    triggers: List[str] = []
    if row.get("sweep_long") and row.get("bull_div"):  triggers.append("VWAP sweep + Bull Div")
    if row.get("sweep_short") and row.get("bear_div"): triggers.append("VWAP sweep + Bear Div")
    if bool(row.get("mom_pop")):                        triggers.append("Momentum Pop")
    if not triggers:
        return

    side = "LONG" if (row.get("sweep_long") or row.get("bull_div")) else "SHORT"
    price = float(row.get("close"))
    vwap  = float(row.get("vwap"))
    rsi   = float(row.get("rsi"))
    score = float(row.get("score"))

    # policy context
    reason = " | ".join(triggers)
    ctx = dict(
        symbol=symbol,
        venue=venue,
        signal_type="spot",
        interval=cfg.interval,
        side=side,
        score=score,
        reason=reason,
        triggers=list(triggers),
        close=price,
        vwap=vwap,
        rsi=rsi,
        ts=_utc_now_iso(),
    )

    dec = POLICY.should_trade(**ctx)
    if not dec.take:
        return

    # global cooldown on venue+symbol (coexists with policy’s buckets)
    now = time.time()
    key = (venue, symbol)
    if now - LAST_ALERT.get(key, 0.0) < max(5, int(cfg.alert_cooldown_sec)):
        return
    LAST_ALERT[key] = now

    # build DB row (columns must match your 'signals' table)
    signal_row = {
        "ts": ctx["ts"],
        "signal_type": "spot",
        "venue": venue,
        "symbol": symbol,
        "interval": cfg.interval,
        "side": side,
        "price": price,
        "vwap": vwap,
        "rsi": rsi,
        "score": score,
        "triggers": triggers,
    }

    # Discord (best effort)
    try:
        await post_signal_embed(
            cfg.discord_webhook,
            exchange=venue,
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
        log.warning(f"discord post failed: {e}")

    # Paper trade + execution row
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

    await _log_to_supa(supa, signal_row, exec_row)

# ------------------------------------------------------------------------------
# BASIS (SPOT-PERP) SIGNALS
# ------------------------------------------------------------------------------
async def _spot_perp_for_symbol(cfg, supa: Optional[Supa], venue: str, symbol: str, executor: PaperExecutor):
    pair = PERP_VENUES.get(venue)
    if not pair:
        return

    spot_cls, perp_cls = pair
    spot, perp = spot_cls(), perp_cls()

    s_df = await _fetch_symbol(spot, symbol, cfg.interval, cfg.lookback)
    p_df = await _fetch_symbol(perp, symbol, cfg.interval, cfg.lookback)
    if len(s_df) < 50 or len(p_df) < 50:
        return

    sig = compute_basis_signals(s_df, p_df, z_win=50, z_th=cfg.spot_perp_z)
    if not sig.get("ok") or not sig["triggers"]:
        return

    # Compute a score from basis Z but DO NOT post unknown columns to Supabase
    side  = sig["side"] or ("SHORT" if sig["basis_pct"] > 0 else "LONG")
    score = 2.0 + min(abs(float(sig["basis_z"])), 5.0)

    price = float(sig["spot_close"])
    vwap  = float(sig["spot_vwap"])
    rsi   = float(sig["spot_rsi"])
    triggers = list(sig["triggers"])
    reason = "Basis:" + ", ".join(triggers)

    ctx = dict(
        symbol=symbol,
        venue=venue,
        signal_type="basis",
        interval=cfg.interval,
        side=side,
        score=score,
        reason=reason,
        triggers=triggers,
        close=price,
        vwap=vwap,
        rsi=rsi,
        ts=_utc_now_iso(),
    )
    dec = POLICY.should_trade(**ctx)
    if not dec.take:
        return

    # simple venue+symbol guard
    now = time.time()
    key = (venue, symbol)
    if now - LAST_ALERT.get(key, 0.0) < max(5, int(cfg.alert_cooldown_sec)):
        return
    LAST_ALERT[key] = now

    # signals table row (keep only columns that exist!)
    signal_row = {
        "ts": ctx["ts"],
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
    }

    # Discord (show basis info only in the embed fields; not sent to DB)
    try:
        await post_signal_embed(
            cfg.discord_webhook,
            exchange=f"{venue}:BASIS",
            symbol=symbol,
            interval=cfg.interval,
            side=side,
            price=price,
            vwap=vwap,
            rsi=rsi,
            score=score,
            triggers=triggers,
            basis_pct=float(sig["basis_pct"]),
            basis_z=float(sig["basis_z"]),
        )
    except Exception as e:
        log.warning(f"discord post (basis) failed: {e}")

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

    await _log_to_supa(supa, signal_row, exec_row)

# ------------------------------------------------------------------------------
# SCANNERS
# ------------------------------------------------------------------------------
async def scan_once():
    cfg = load_settings()
    supa = get_supa(cfg)  # reuse singleton
    executor = PaperExecutor(cfg.max_pos_usdt)

    # Build per-venue hotlist (or just use configured symbols)
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

    # Spot scanner
    for venue, ex_cls in SPOT_ADAPTERS.items():
        if venue not in cfg.exchanges:
            continue
        ex = ex_cls()
        for sym in (hotmap.get(venue, []) or list(cfg.symbols)):
            await _process_symbol(cfg, supa, venue, ex, sym, executor)

    # Basis scanner
    if cfg.spot_perp_enabled:
        for venue in cfg.spot_perp_exchanges:
            for sym in (hotmap.get(venue, []) or list(cfg.symbols)):
                await _spot_perp_for_symbol(cfg, supa, venue, sym, executor)

# ------------------------------------------------------------------------------
# MAIN LOOP
# ------------------------------------------------------------------------------
async def main_loop():
    cfg = load_settings()
    period = max(10, int(cfg.scan_period_sec))
    supa = get_supa(cfg)  # ensure created before loop (and closed after)
    log.info(f"Starting scan loop | exchanges={cfg.exchanges} interval={cfg.interval} period={period}s")
    try:
        while True:
            await scan_once()
            await asyncio.sleep(period)
    finally:
        if supa:
            await supa.close()

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main_loop())
