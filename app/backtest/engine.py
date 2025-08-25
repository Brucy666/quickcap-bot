# app/backtest/engine.py
import asyncio, time
from datetime import datetime, timezone
from typing import Dict, Tuple, List
import pandas as pd

from app.config import load_settings
from app.utils import to_dataframe
from app.signals import compute_signals
from app.scoring import score_row
from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic
)
from app.storage.sqlite_store import SQLiteStore
from app.storage.supabase import Supa

SPOT = {
    "kucoin": KuCoinPublic,
    "mexc": MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx": OKXSpotPublic,
    "bybit": BybitSpotPublic,
}

class BacktestEngine:
    def __init__(self, venue: str, symbols: List[str], interval: str = "1m", lookback: int = 1000,
                 alert_min_score: float = 2.3, cooldown_sec: int = 180, sqlite_path: str = "quickcap_results.db"):
        self.venue = venue
        self.symbols = symbols
        self.interval = interval
        self.lookback = lookback
        self.alert_min_score = alert_min_score
        self.cooldown_sec = cooldown_sec
        self.last_alert: Dict[Tuple[str, str], float] = {}
        self.store = SQLiteStore(sqlite_path)

        cfg = load_settings()
        self.supa = None
        if getattr(cfg, "supabase_enabled", False) and cfg.supabase_url and cfg.supabase_key:
            self.supa = Supa(cfg.supabase_url, cfg.supabase_key)

        cls = SPOT.get(venue)
        if not cls: raise SystemExit(f"Unsupported venue '{venue}'")
        self.ex = cls()

    async def _fetch(self, symbol: str) -> pd.DataFrame:
        try:
            kl = await self.ex.fetch_klines(symbol, self.interval, self.lookback)
        except Exception:
            kl = []
        return to_dataframe(kl)

    async def run_symbol(self, symbol: str):
        df = await self._fetch(symbol)
        if len(df) < 50:
            return 0,0
        # replay bar-by-bar to avoid lookahead
        n_sig = n_exec = 0
        for i in range(50, len(df)):
            window = df.iloc[:i+1].copy()
            sig = compute_signals(window)
            last = sig.iloc[-1].copy()
            last["score"] = float(score_row(last))

            # triggers (same as live logic)
            triggers: list[str] = []
            if last.get("sweep_long") and last.get("bull_div"):  triggers.append("VWAP sweep + Bull Div")
            if last.get("sweep_short") and last.get("bear_div"): triggers.append("VWAP sweep + Bear Div")
            if bool(last.get("mom_pop")):                        triggers.append("Momentum Pop")

            if triggers and last["score"] >= self.alert_min_score:
                now = window["ts"].iloc[-1].timestamp()
                key = (self.venue, symbol)
                if now - self.last_alert.get(key, 0) < self.cooldown_sec:
                    continue
                self.last_alert[key] = now
                side = "LONG" if (last.get("sweep_long") or last.get("bull_div")) else "SHORT"

                row = {
                    "ts": datetime.utcfromtimestamp(now).replace(tzinfo=timezone.utc).isoformat(),
                    "signal_type": "spot",
                    "venue": self.venue,
                    "symbol": symbol,
                    "interval": self.interval,
                    "side": side,
                    "price": float(last["close"]),
                    "vwap": float(last["vwap"]),
                    "rsi": float(last["rsi"]),
                    "score": float(last["score"]),
                    "triggers": triggers,
                }
                signal_id = self.store.insert_signal(row); n_sig += 1

                # paper execution @ next bar open if available
                if i+1 < len(df):
                    exec_row = {
                        "ts": datetime.utcfromtimestamp(df.iloc[i+1]["ts"].timestamp()).replace(tzinfo=timezone.utc).isoformat(),
                        "venue": "PAPER",
                        "symbol": symbol,
                        "side": side,
                        "price": float(df.iloc[i+1]["open"]),
                        "score": float(last["score"]),
                        "reason": ", ".join(triggers),
                        "is_paper": True,
                    }
                    self.store.insert_execution(exec_row); n_exec += 1

                    if self.supa:
                        # non-blocking: mirror to Supabase if configured
                        asyncio.create_task(self.supa.log_signal(**row))
                        asyncio.create_task(self.supa.log_execution(**exec_row))
        return n_sig, n_exec

    async def run(self):
        totals = [await self.run_symbol(s) for s in self.symbols]
        return dict(signals=sum(x for x,_ in totals), executions=sum(y for _,y in totals))
