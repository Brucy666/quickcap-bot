from .logger import get_logger
log = get_logger("executor")

class PaperExecutor:
    def __init__(self, max_usdt: float = 200.0):
        self.max_usdt = max_usdt

    async def submit(self, symbol: str, side: str, price: float, score: float, reason: str):
        # Placeholder paper-trade action (no live orders)
        log.info(f"[PAPER] {symbol} {side} @ {price:.4f} (score={score}) :: {reason}")
