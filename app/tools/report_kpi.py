import asyncio
from datetime import datetime, timezone
from app.notifier import post_performance_text
from app.storage.supabase import Supa
from app.config import load_settings

async def run_kpi_report():
    """Aggregate outcomes + post KPI summary to Discord."""
    cfg = load_settings()
    supa = Supa(cfg.supabase_url, cfg.supabase_key)

    # Pull recent outcomes (last 24h UTC)
    now = datetime.now(timezone.utc)
    since = (now.timestamp() - 86400) * 1000  # ms
    rows = await supa.fetch("signal_outcomes", since_ts=since)

    if not rows:
        await post_performance_text("No outcomes recorded in the last 24h.")
        return

    total = len(rows)
    wins = sum(1 for r in rows if r["ret"] > 0)
    winrate = wins / total if total else 0

    # Quick expectancy calc
    exp = sum(r["ret"] for r in rows) / total

    msg = (
        f"📊 **Daily KPI Report**\n"
        f"Signals evaluated: **{total}**\n"
        f"Winrate: **{winrate:.1%}**\n"
        f"Expectancy: **{exp:.4f}**\n"
        f"(Last 24h, across all triggers/horizons)"
    )
    await post_performance_text(msg)

if __name__ == "__main__":
    asyncio.run(run_kpi_report())
