import asyncio
import time
from typing import Dict, Tuple

from app.config import load_settings
from app.logger import get_logger
from app.utils import to_dataframe
from app.exchanges import KuCoinPublic, MEXCPublic
from app.signals import compute_signals
from app.scoring import score_row
from app.executor import PaperExecutor
from app.notifier import post_signal_embed

log = get_logger("main")
LAST_ALERT: Dict[Tuple[str, str], float] = {}

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
        if len(df) < 50:
            log.info(f"{ex_name}:{symbol} insufficient candles ({len(df)})")
            return

        sig = compute_signals(df)
        last = sig.iloc[-1].copy()
        last["score"] = float(score_row(last))

        triggers = []
        if last.get("sweep_long") and last.get("bull_div"):  triggers.append("VWAP sweep + Bull Div")
        if last.get("sweep_short") and last.get("bear_div"): triggers.append("VWAP sweep + Bear Div")
        if bool(last.get("mom_pop")):                        triggers.append("Momentum Pop")

        if cfg.risk_off:
            log.info(f"{ex_name}:{symbol} risk_off=True | price={last['close']:.4f}")
            return

        if triggers and last["score"] >= cfg.alert_min_score:
            now = time.time(); key = (ex_name, symbol)
            if now - LAST_ALERT.get(key, 0) < cfg.alert_cooldown_sec:
                log.info(f"{ex_name}:{symbol} skipped (cooldown)")
                return
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
        else:
            vwap_delta = (last["close"] - last["vwap"]) / last["vwap"] * 100.0
            log.info(f"{ex_name}:{symbol} ok | price={last['close']:.4f} rsi={last['rsi']:.1f} vwapΔ={vwap_delta:.2f}% score={last['score']}")
    except Exception as e:
        log.error(f"{ex_name}:{symbol} error: {e}")

async def scan_once():
    cfg = load_settings()
    exes = _build_exchanges(cfg.exchanges)
    executor = PaperExecutor(cfg.max_pos_usdt)
    for ex_name, ex in exes:
        for sym in cfg.symbols:
            await _process_symbol(cfg, ex_name, ex, sym, executor)

async def main_loop():
    cfg = load_settings()
    period = max(10, int(cfg.scan_period_sec))
    log.info(f"Starting scan loop | exchanges={cfg.exchanges} symbols={cfg.symbols} interval={cfg.interval} period={period}s")
    while True:
        await scan_once()
        await asyncio.sleep(period)

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main_loop())
