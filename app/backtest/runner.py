# app/backtest/runner.py
import asyncio, argparse, json
from app.backtest.engine import BacktestEngine
from app.backtest.metrics import compute_outcomes_sqlite
from app.storage.sqlite_store import SQLiteStore

async def main():
    p = argparse.ArgumentParser(description="QuickCap backtester")
    p.add_argument("--venue", default="kucoin", help="kucoin|binance|okx|bybit|mexc")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--lookback", type=int, default=1000)
    p.add_argument("--score", type=float, default=2.3, help="alert_min_score")
    p.add_argument("--cooldown", type=int, default=180)
    p.add_argument("--sqlite", default="quickcap_results.db")
    args = p.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    engine = BacktestEngine(
        venue=args.venue, symbols=symbols, interval=args.interval, lookback=args.lookback,
        alert_min_score=args.score, cooldown_sec=args.cooldown, sqlite_path=args.sqlite,
    )
    totals = await engine.run()

    store = SQLiteStore(args.sqlite)
    out_counts = 0
    for sym in symbols:
        out_counts += await compute_outcomes_sqlite(args.venue, sym, args.interval, args.lookback, store)

    print(json.dumps({
        "signals": totals["signals"],
        "executions": totals["executions"],
        "outcome_rows": out_counts
    }))

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
