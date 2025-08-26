# app/main.py
from __future__ import annotations

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import pandas as pd
pd.set_option("future.no_silent_downcasting", True)

import asyncio
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
from app.policy import POLICY  # <<< central trade filter

# PUBLIC exchange connectors
from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic,
    BinancePerpPublic, OKXPerpPublic, BybitPerpPublic,
)

log = get_logger("main")

# ---------------- registry ----------------

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

# --------------- helpers -----------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _build_spot_exchanges(enabled: List[str]):
    return [(name, SPOT_ADAPTERS[name]()) for name in SPOT_ADAPTERS if name in enabled]

def _supa(cfg):
    """
    Create a Supa client if enabled; otherwise None.
    """
    enabled = getattr(cfg, "supabase_enabled", False)
    url     = getattr(cfg, "supabase_url", "") or ""
    key     = getattr(cfg, "supabase_key", "") or ""
    if enabled and url and key:
        try:
            return Supa(url, key)
        except Exception as e:
            log.warning(f"Supabase init failed: {e}")
    return None

async def _fetch_symbol(ex, symbol: str, interval: str, lookback: int):
    """
    Fetch klines and normalize to a DataFrame with at least: ts, open, high, low, close, vwap, rsi (if computed downstream).
    """
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
        return to_dataframe(kl)
    except Exception as e:
        log.warning(f"fetch_klines error {type(ex).__name__}:{symbol}:{interval}: {e}")
        return to_dataframe([])

async def _log_signal_and_exec_to_supa(
    supa: Supa | None,
    signal_payload: dict,
    exec_payload: dict | None = None,
):
    """
    Fire-and-forget inserts to Supabase.
    """
    if not supa:
        return
    try:
        asyncio.create_task(supa.log_signal(**signal_payload))
        if exec_payload:
            asyncio.create_task(supa.log_execution(**exec_payload))
    except Exception as e:
        log.error(f"Supabase log error: {e}")

# --------------- spot (VWAP/RSI/div) ---------------

async def _process_symbol(cfg, supa: Supa | None, ex_name: str, ex, symbol: str, executor: PaperExecutor):
    """
    Evaluate spot signal for a single symbol, gate with POLICY, notify, paper-exec, log.
    """
    df = await _fetch_symbol(ex, symbol, cfg.interval, cfg.lookback)
    if len(df) < 50 or getattr(cfg, "risk_off", False):
        return

    sig = compute_signals(df)
    last = sig.iloc[-1].copy()

    # compute score
    last_score = float(score_row(last))
    last["score"] = last_score

    # triggers and reason
    triggers: List[str] = []
    if last.get("sweep_long"):   triggers.append("VWAP Sweep Long")
    if last.get("sweep_short"):  triggers.append("VWAP Sweep Short")
    if last.get("bull_div"):     triggers.append("Bull Div")
    if last.get("bear_div"):     triggers.append("Bear Div")
    if last.get("mom_pop"):      triggers.append("Momentum Pop")
    if not triggers:
        return

    # side heuristic
    side = "LONG"
    if last.get("sweep_short") or last.get("bear_div"):
        side = "SHORT"
    if last.get("sweep_long") or last.get("bull_div"):
        side = "LONG"

    price = float(last.get("close", df["close"].iloc[-1]))
    vwap  = float(last.get("vwap",  df.get("vwap", df["close"]).iloc[-1]))
    rsi   = float(last.get("rsi", 0.0))
    reason = ", ".join(triggers)

    # ---------- POLICY gate ----------
    dec = POLICY.should_trade(
        symbol=symbol,
        side=side,
        score=float(last_score),
        reason=reason,
        signal_type="spot",
        rsi=rsi,
        vwap=vwap,
        close=price,
        triggers=list(triggers),
    )
    if not dec.take:
        return

    # Discord
    try:
        await post_signal_embed(
            cfg.discord_webhook,
            exchange=ex_name,
            symbol=symbol,
            interval=cfg.interval,
            side=side,
            price=price,
            vwap=vwap,
            rsi=rsi,
            score=float(last_score),
            triggers=list(triggers),
        )
    except Exception as e:
        log.warning(f"Discord (spot) failed: {e}")

    # Paper exec
    exec_rec = await executor.submit(
        symbol=symbol,
        side=side,
        price=price,
        score=float(last_score),
        reason=reason,
    )
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

    # DB payload (no funky extra columns)
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
        "close": price,
    }

    await _log_signal_and_exec_to_supa(supa, signal_row, exec_row)

# --------------- basis (spot-perp) ---------------

async def _spot_perp_for_symbol(cfg, supa: Supa | None, venue: str, symbol: str, executor: PaperExecutor):
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
    if not sig.get("ok") or not sig.get("triggers"):
        return

    side  = sig.get("side") or ("SHORT" if float(sig["basis_pct"]) > 0 else "LONG")
    score = 2.0 + min(abs(float(sig["basis_z"])), 5.0)
    reason = "Basis:" + ",".join(sig["triggers"])

    # ---------- POLICY gate ----------
    dec = POLICY.should_trade(
        symbol=symbol,
        side=side,
        score=float(score),
        reason=reason,
        signal_type="basis",
        rsi=float(sig["spot_rsi"]),
        vwap=float(sig["spot_vwap"]),
        close=float(sig["spot_close"]),
        triggers=list(sig["triggers"]),
    )
    if not dec.take:
        return

    # Discord (basis metrics allowed in the embed only)
    try:
        await post_signal_embed(
            cfg.discord_webhook,
            exchange=f"{venue}:BASIS",
            symbol=symbol,
            interval=cfg.interval,
            side=side,
            price=float(sig["spot_close"]),
            vwap=float(sig["spot_vwap"]),
            rsi=float(sig["spot_rsi"]),
            score=float(score),
            triggers=list(sig["triggers"]),
            basis_pct=float(sig["basis_pct"]),
            basis_z=float(sig["basis_z"]),
        )
    except Exception as e:
        log.warning(f"Discord (basis) failed: {e}")

    # Paper exec
    exec_rec = await executor.submit(
        symbol=symbol,
        side=side,
        price=float(sig["spot_close"]),
        score=float(score),
        reason=reason,
    )
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

    # DB payload — do NOT include basis_pct / basis_z (not in your signals table)
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
        "reason": reason,
        "close": float(sig["spot_close"]),
    }

    await _log_signal_and_exec_to_supa(supa, signal_row, exec_row)

# --------------- scanner loop ---------------

async def scan_once():
    cfg = load_settings()
    supa = _supa(cfg)
    executor = PaperExecutor(cfg.max_pos_usdt)

    # hotlist
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
