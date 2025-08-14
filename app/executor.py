# inside PaperExecutor.submit(...)
from app.config import load_settings
from app.storage.supabase import Supa

# after your existing log line:
try:
    cfg = load_settings()
    if cfg.supabase_enabled and cfg.supabase_url and cfg.supabase_key:
        supa = Supa(cfg.supabase_url, cfg.supabase_key)
        await supa.log_execution(
            venue="PAPER", symbol=symbol, side=side, price=price,
            score=score, reason=reason, is_paper=True
        )
except Exception:
    pass
