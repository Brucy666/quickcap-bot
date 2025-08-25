worker: python -m app.main
backfill: python -m app.backfill.backfill --venue kucoin --symbols BTCUSDT,ETHUSDT --interval 1m --lookback 50000 --score 2.3 --cooldown 180
report: python -m app.tools.report_to_discord
