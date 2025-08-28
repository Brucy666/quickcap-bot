# app/main.py
from __future__ import annotations

# --- quiet pandas' FutureWarning noise (we still opt into no_silent_downcasting) ---
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
from app.alpha.spot_perp_engine import compute_basis_signals

from app.storage.supabase import Supa
from app.policy import POLICY                  # centralized trade policy / cooldowns
from app.notifier import NOTIFY                # Discord notifier (singleton)

from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic,
    BinancePerpPublic, OKXPerpPublic, BybitPerpPublic,
)

log = get_logger("main")

# keep a very lightweight local cooldown as an extra guard
_LAST_ALERT: Dict[Tuple[str, str], float] = {}

SPOT_ADAPTERS = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}
PERP_VENUES = {
    "binance": (BinanceSpotPublic,  BinancePerpPublic),
    "okx":     (OKXSpotPublic,      OKXPerpPublic),
    "bybit":   (BybitSpotPublic,    BybitPerpPublic),
}

# ---------------- util ----------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _mk_reason(triggers: List[str]) -> str:
    return ", ".join(t for t in triggers if str(t).strip())

def _supa(cfg) -> Optional[Supa]:
    try:
        if getattr(cfg, "supabase_enabled", False) and cfg.supabase_url and cfg.supabase_key:
            return Supa(cfg.supabase_url, cfg.supabase_key)
    except Exception as e:
        log.warning(f"Supabase init skipped: {e}")
    return None

def _spot_clients(enabled: List[str]):
    out = []
    for name, cls in SPOT_ADAPTERS.items():
        if name in enabled:
            try:
                out.append((name, cls()))
            except Exception as e:
                log.warning(f"init {name} failed: {e}")
    return out

async def _fetch_df(ex, symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    """Fetch klines and normalize; always returns a DataFrame."""
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception as e:
        log.debug(f"fetch_klines error {getattr(ex, 'name', ex)} {symbol}: {e}")
        kl = []
    df = to_dataframe(kl)
    # drop obviously-bad rows
    if "close" in df:
        df = df[pd.to_numeric(df["close"], errors="coerce").notna()]
    return df.reset_index(drop=True)

async def _log_to_supa(supa: Optional[Supa], signal_row: dict, exec_row: Optional[dict] = None):
    if not supa:
        return
    try:
        asyncio.create_task(supa.log_signal(**signal_row))
        if exec_row:
            asyncio.create_task(supa.log_execution(**exec_row))
    except Exception as e:
        log.warning(f"supa log error: {e}")

# ---------------- spot path ----------------

async def _process_symbol(cfg, supa: Optional[Supa], ex_name: str, ex, symbol: str, executor: PaperExecutor):
    df = await _fetch_df(ex, symbol, cfg.interval, cfg.lookback)
    if len(df) < 50 or getattr(cfg, "risk_off", False):
        return

    sig = compute_signals(df)
    last = sig.iloc[-1].copy()
    last["score"] = float(score_row(last))

    # triggers
    triggers: List[str] = []
    if last.get("sweep_long") or last.get("bull_div"):
        triggers.append("VWAP Sweep Long") if last.get("sweep_long") else None
    if last.get("sweep_short") or last.get("bear_div"):
        triggers.append("VWAP Sweep Short") if last.get("sweep_short") else None
    if bool(last.get("mom_pop")):
        triggers.append("Momentum Pop")
    if not triggers:
        return

    side  = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"
    price = float(last.get("close", df["close"].iloc[-1]))
    vwap  = float(last.get("vwap",  df.get("vwap", df["close"]).iloc[-1]))
    rsi   = float(last.get("rsi",  0.0))
    score = float(last["score"])
    reason = _mk_reason(triggers)

    # policy gate (FIX: pass kwargs, not positional)
    ctx = dict(
        signal_type="spot",
        venue=ex_name,
        symbol=symbol,
        interval=cfg.interval,
        side=side,
        score=score,
        reason=reason,
        ts=_utc_now_iso(),
        close=price, vwap=vwap, rsi=rsi,
        triggers=triggers,
    )
    dec = POLICY.should_trade(**ctx)
    if not dec.take:
        return

    # local extra cooldown (per-venue+symbol)
    now = time.time(); key = (ex_name, symbol)
    if now - _LAST_ALERT.get(key, 0.0) < max(5, int(getattr(cfg, "alert_cooldown_sec", 0) or 0)):
        return
    _LAST_ALERT[key] = now

    # signal payload
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

    # discord (live)
    try:
        await NOTIFY.signal_embed(
            exchange=ex_name, symbol=symbol, interval=cfg.interval, side=side,
            price=price, vwap=vwap, rsi=rsi, score=score, triggers=triggers,
        )
    except Exception as e:
        log.warning(f"notify live (spot) failed: {e}")

    # paper exec
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

# ---------------- basis path ----------------

async def _spot_perp_for_symbol(cfg, supa: Optional[Supa], venue: str, symbol: str, executor: PaperExecutor):
    pair = PERP_VENUES.get(venue)
    if not pair:
        return
    spot_cls, perp_cls = pair
    spot = spot_cls(); perp = perp_cls()

    s_df = await _fetch_df(spot, symbol, cfg.interval, cfg.lookback)
    p_df = await _fetch_df(perp, symbol, cfg.interval, cfg.lookback)
    if len(s_df) < 50 or len(p_df) < 50:
        return

    sig = compute_basis_signals(s_df, p_df, z_win=50, z_th=getattr(cfg, "spot_perp_z", 2.5))
    if not sig.get("ok") or not sig.get("triggers"):
        return

    side = sig.get("side") or ("SHORT" if float(sig["basis_pct"]) > 0 else "LONG")
    score = 2.0 + min(abs(float(sig["basis_z"])), 5.0)
    triggers = list(sig["triggers"])
    reason = "Basis:" + _mk_reason(triggers)

    # policy (FIX: kwargs)
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
    dec = POLICY.should_trade(**ctx)
    if not dec.take:
        return

    # signal payload
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

    # discord (live + basis fields)
    try:
        await NOTIFY.signal_embed(
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
        log.warning(f"notify live (basis) failed: {e}")

    # paper exec
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

    await _log_to_supa(supa, signal_row, exec_row)

# ---------------- orchestration ----------------

async def scan_once():
    cfg = load_settings()
    supa = _supa(cfg)
    executor = PaperExecutor(getattr(cfg, "max_pos_usdt", 0))

    # hotlist (or static symbols)
    if getattr(cfg, "hotlist_enabled", False):
        hotmap = await build_hotmap(
            cfg.exchanges,
            top_n=getattr(cfg, "hotlist_top_n", 20),
            min_vol_usdt=getattr(cfg, "hotlist_min_vol_usdt", 1_000_000),
            force_symbols=getattr(cfg, "force_symbols", []),
            exclude_symbols=getattr(cfg, "exclude_symbols", []),
        )
        log.info("Hotlist per-venue: { " + ", ".join(f"{k}:{len(v)}" for k, v in hotmap.items()) + " }")
    else:
        hotmap = {ex: list(getattr(cfg, "symbols", [])) for ex in cfg.exchanges}

    # SPOT
    for ex_name, ex in _spot_clients(cfg.exchanges):
        for sym in (hotmap.get(ex_name, []) or list(getattr(cfg, "symbols", []))):
            await _process_symbol(cfg, supa, ex_name, ex, sym, executor)

    # BASIS
    if getattr(cfg, "spot_perp_enabled", False):
        for venue in getattr(cfg, "spot_perp_exchanges", []):
            for sym in (hotmap.get(venue, []) or list(getattr(cfg, "symbols", []))):
                await _spot_perp_for_symbol(cfg, supa, venue, sym, executor)

async def main_loop():
    cfg = load_settings()
    period = max(10, int(getattr(cfg, "scan_period_sec", 60)))
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
