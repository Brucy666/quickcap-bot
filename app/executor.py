# app/executor.py
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional
from app.logger import get_logger

log = get_logger("executor")

@dataclass
class PaperExecutor:
    max_pos_usdt: float = 1000.0

    async def submit(self, symbol: str, side: str, price: float, score: float, reason: str) -> dict:
        """
        Paper 'execution' stub. Returns a record so callers can forward it to analytics/loggers.
        """
        ts = time.time()
        rec = {
            "ts": ts,
            "venue": "PAPER",
            "symbol": symbol,
            "side": side.upper(),
            "price": float(price),
            "score": float(score),
            "reason": reason,
            "is_paper": True,
        }
        log.info(f"[PAPER] {rec['symbol']} {rec['side']} @ {rec['price']} (score={rec['score']}) :: {reason}")
        return rec
