from app.logger import get_logger
from app.config import load_settings
from app.storage.supabase import Supa

log = get_logger("executor")

class PaperExecutor:
    def __init__(self, max_usdt: float):
        self.max_usdt = max_usdt

    async def submit(self, symbol: str, side: str, price: float, score: float, reason: str):
        # Log to console
        log.info(f"[PAPER] {symbol} {side} @ {price:.4f} (score={score}) :: {reason}")

        # Supabase logging
        try:
            cfg = load_settings()
            if cfg.supabase_enabled and cfg.supabase_url and cfg.supabase_key:
                supa = Supa(cfg.supabase_url, cfg.supabase_key)
                await supa.log_execution(
                    venue="PAPER",
                    symbol=symbol,
                    side=side,
                    price=price,
                    score=score,
                    reason=reason,
                    is_paper=True
                )
        except Exception as e:
            log.error(f"Supabase log_execution error: {e}")
