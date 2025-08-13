import asyncio
import pandas as pd
from app.config import load_settings
from app.logger import get_logger
from app.utils import to_dataframe
from app.exchanges import KuCoinPublic, MEXCPublic
from app.signals import compute_signals
from app.scoring import score_row
from app.executor import PaperExecutor
from app.notifier import post_discord

log = get_logger("main")

async def fetch_symbol(ex, symbol, interval, lookback):
    kl = await ex.fetch_klines(symbol, interval, lookback)
    return to_dataframe(kl)

async def scan_once():
    cfg = load_settings()
    exes = []
    if "kucoin" in cfg.exchanges:
        exes.append(("kucoin", KuCoinPublic()))
    if "mexc" in cfg.exchanges:
        exes.append(("mexc", MEXCPublic()))

    execu = PaperExecutor(cfg.max_pos_usdt)

    for ex_name, ex in exes:
        for sym in cfg.symbols:
            try:
                df = await fetch_symbol(ex, sym, cfg.interval, cfg.lookback)
                if len(df) < 50:
                    continue
                sig = compute_signals(df)
                last = sig.iloc[-1].copy()
                last["score"] = score_row(last)

                # Alert conditions (tune as desired)
                triggers = []
                if last["sweep_long"] and last["bull_div"]:
                    triggers.append("VWAP sweep + Bull Div")
                if last["sweep_short"] and last["bear_div"]:
                    triggers.append("VWAP sweep + Bear Div")
                if last["mom_pop"]:
                    triggers.append("Momentum Pop")

                if triggers and not cfg.risk_off:
                    side = "LONG" if last["sweep_long"] or last["bull_div"] else "SHORT"
                    text = (
                        f"**{ex_name.upper()} | {sym} | {cfg.interval}**\n"
                        f"Price: `{last['close']:.4f}` | VWAP: `{last['vwap']:.4f}` | RSI: `{last['rsi']:.1f}`\n"
                        f"Triggers: {', '.join(triggers)}\n"
                        f"Score: **{last['score']}**"
                    )
                    await post_discord(cfg.discord_webhook, text)
                    await execu.submit(sym, side, float(last["close"]), float(last["score"]), ", ".join(triggers))
                else:
                    log.info(f"{ex_name}:{sym} ok | price={last['close']:.4f} rsi={last['rsi']:.1f} vwapΔ={(last['close']-last['vwap'])/last['vwap']*100:.2f}% score={last['score']}")
            except Exception as e:
                log.error(f"{ex_name}:{sym} error: {e}")

async def main_loop():
    while True:
        await scan_once()
        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except Exception:
        pass
    asyncio.run(main_loop())
