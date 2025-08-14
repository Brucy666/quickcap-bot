# Silence warnings & set pandas options
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import pandas as pd
pd.set_option("future.no_silent_downcasting", True)

import asyncio
import time
from typing import Dict, Tuple

from app.config import load_settings
from app.logger import get_logger
from app.utils import to_dataframe
from app.hotlist import build_hotlist
from app.exchanges import KuCoinPublic, MEXCPublic
from app.scoring import score_row
from app.signals import compute_signals
from app.executor import PaperExecutor
from app.notifier import post_signal_embed
from app.alpha.spot_perp_engine import compute_basis_signals
from app.exchanges import (
    BinanceSpotPublic, BinancePerpPublic,
    OKXSpotPublic, OKXPerpPublic,
    BybitSpotPublic, BybitPerpPublic,
)

log = get_logger("main")
LAST_ALERT: Dict[Tuple[str, str], float] = {}

VENUES = {
    "binance": (BinanceSpotPublic, BinancePerpPublic),
    "okx":     (OKXSpotPublic, OKXPerpPublic),
    "bybit":   (BybitSpotPublic, BybitPerpPublic),
}

def _build_exchanges(enabled: list[str]):
    exes = []
    if "kucoin" in enabled: exes.append(("kucoin", KuCoinPublic()))
    if "mexc" in enabled:   exes.append(("mexc", MEXCPublic()))
    return exes

async def _fetch_symbol(ex, symbol: str, interval: str, lookback: int):
    kl = await ex.fetch_klines(symbol, interval, lookback)
    return to_dataframe(kl)

async def _process_symbol(cfg, ex_name: str, ex, symbol: str, executor: PaperExecutor):
    try:
        df = await _fetch_symbol(ex, symbol, cfg.interval, cfg.lookback)
        if len(df) < 50: return
        sig = compute_signals(df)
        last = sig.iloc[-1].copy()
        last["score"] = float(score_row(last))
        triggers = []
        if last.get("sweep_long") and last.get("bull_div"):  triggers.append("VWAP sweep + Bull Div")
        if last.get("sweep_short") and last.get("bear_div"): triggers.append("VWAP sweep + Bear Div")
        if bool(last.get("mom_pop")):                        triggers.append("Momentum Pop")
        if cfg.risk_off: return
        if triggers and last["score"] >= cfg.alert_min_score:
            now = time.time(); key = (ex_name, symbol)
            if now - LAST_ALERT.get(key, 0) < cfg.alert_cooldown_sec: return
            LAST_ALERT[key] = now
            side = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"
            await post_signal_embed(
                cfg.discord_webhook,
                exchange=ex_name,
                symbol=symbol,
                interval=cfg.interval,
                side=side,
                price=float(last["close"]),
                vwap=float(last["vwap"]),
                rsi=float(last["rsi"]),
                score=float(last["score"]),
                triggers=triggers,
            )
            await executor.submit(symbol, side, float(last["close"]), float(last["score"]), ", ".join(triggers))
    except Exception as e:
        log.error(f"{ex_name}:{symbol} error: {e}")

async def _spot_perp_for_symbol(cfg, venue: str, symbol: str, executor):
    try:
        spot_cls, perp_cls = VENUES[venue]
    except KeyError:
        return
    spot = spot_cls(); perp = perp_cls()
    s_df = await _fetch_symbol(spot, symbol, cfg.interval, cfg.lookback)
    p_df = await _fetch_symbol(perp, symbol, cfg.interval, cfg.lookback)
    sig = compute_basis_signals(s_df, p_df, z_win=50, z_th=cfg.spot_perp_z)
    if not sig.get("ok") or not sig["triggers"]: return
    side = sig["side"] or ("SHORT" if sig["basis_pct"] > 0 else "LONG")
    score = 2.0 + min(abs(sig["basis_z"]), 5.0)
    await post_signal_embed(
        cfg.discord_webhook,
        exchange=f"{venue}:BASIS",
        symbol=symbol,
        interval=cfg.interval,
        side=side,
        price=sig["spot_close"],
        vwap=sig["spot_vwap"],
        rsi=sig["spot_rsi"],
        score=score,
        triggers=sig["triggers"],
        basis_pct=sig["basis_pct"],
        basis_z=sig["basis_z"],
    )
    await executor.submit(symbol, side, float(sig["spot_close"]), float(score), "Basis:" + ",".join(sig["triggers"]))

async def scan_once():
    cfg = load_settings()
    exes = _build_exchanges(cfg.exchanges)
    executor = PaperExecutor(cfg.max_pos_usdt)
    symbols = cfg.symbols
    if cfg.hotlist_enabled:
        try:
            symbols = await build_hotlist(
                cfg.exchanges,
                top_n=cfg.hotlist_top_n,
                min_vol_usdt=cfg.hotlist_min_vol_usdt,
                force_symbols=cfg.force_symbols,
                exclude_symbols=cfg.exclude_symbols,
            )
            log.info(f"Hotlist: {symbols}")
        except Exception as e:
            log.error(f"hotlist error: {e}")
    for ex_name, ex in exes:
        for sym in symbols:
            await _process_symbol(cfg, ex_name, ex, sym, executor)
    if cfg.spot_perp_enabled:
        for venue in cfg.spot_perp_exchanges:
            for sym in symbols:
                await _spot_perp_for_symbol(cfg, venue, sym, executor)

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
