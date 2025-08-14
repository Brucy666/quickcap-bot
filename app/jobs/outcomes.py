import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

from app.storage.supabase import Supa
from app.config import load_settings
from app.logger import get_logger
from app.utils import to_dataframe

# spot adapters by venue (use spot for outcome calc)
from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic,
)

log = get_logger("outcomes")

SPOT = {
    "kucoin": KuCoinPublic,
    "mexc":   MEXCPublic,
    "binance": BinanceSpotPublic,
    "okx":     OKXSpotPublic,
    "bybit":   BybitSpotPublic,
}

HORIZONS_MIN = [5, 15, 30, 60]  # tweak as you like

def _dir(side: str) -> int:
    return 1 if side.upper() == "LONG" else -1

def _nearest_index(df, ts_ms: int) -> int:
    # assume df['ts'] sorted asc (your to_dataframe does this)
    # pick the first bar with ts >= signal ts
    arr = df["ts"].values
    lo, hi = 0, len(arr)-1
    if len(arr) == 0 or ts_ms > arr[-1]:
        return -1
    # linear scan is fine for 500 bars; keep simple
    for i, t in enumerate(arr):
        if t >= ts_ms:
            return i
    return -1

def _calc_window_metrics(df, i0: int, i1: int, side: str, entry_price: float) -> Tuple[float, float, float]:
    # dir-adjusted returns (positive is good for the chosen side)
    d = _dir(side)
    segment = df.iloc[i0:i1+1]
    exit_price = float(segment.iloc[-1]["close"])
    ret = (exit_price / entry_price - 1.0) * d
    hi = float(segment["high"].max())
    lo = float(segment["low"].min())
    max_fav = ((hi / entry_price - 1.0) * d) if d > 0 else ((entry_price / lo - 1.0) * 1)  # normalize
    max_adv = ((entry_price / lo - 1.0) * d) if d > 0 else ((hi / entry_price - 1.0) * 1)  # adverse
    # For SHORT, definitions above translate correctly (but you can simplify if preferred)
    return ret, max_fav, max_adv, exit_price

async def _fetch_df(venue: str, symbol: str, interval: str, lookback: int):
    cls = SPOT.get(venue)
    if not cls:
        return to_dataframe([])
    ex = cls()
    try:
        kl = await ex.fetch_klines(symbol, interval, lookback)
    except Exception:
        kl = []
    return to_dataframe(kl)

async def compute_outcomes():
    cfg = load_settings()
    if not (cfg.supabase_enabled and cfg.supabase_url and cfg.supabase_key):
        log.error("Supabase disabled; aborting outcomes job.")
        return

    supa = Supa(cfg.supabase_url, cfg.supabase_key)

    # Pull last 24h signals that don't yet have outcomes for the largest horizon
    largest = max(HORIZONS_MIN)
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    # Grab candidate signals
    signals = await supa.select(
        "signals",
        {
            "select": "id,ts,signal_type,venue,symbol,side,price",
            "ts": f"gte.{since}",
            "order": "ts.asc",
            "limit": "1000",
        },
    )
    if not signals:
        log.info("No signals to process.")
        return

    # Find which already have outcomes at largest horizon
    existing = await supa.select(
        "signal_outcomes",
        {
            "select": "signal_id,horizon_m",
            "horizon_m": f"eq.{largest}",
            "ts": f"gte.{since}",
            "limit": "2000",
        },
    )
    done_ids = {e["signal_id"] for e in existing}

    # Group signals per (venue, symbol) for efficient fetching
    by_pair: Dict[Tuple[str,str], List[dict]] = {}
    for s in signals:
        if s["id"] in done_ids:  # already computed
            continue
        by_pair.setdefault((s["venue"], s["symbol"]), []).append(s)

    rows = []
    for (venue, symbol), items in by_pair.items():
        # fetch approx 2 hours of 1m bars: enough for 60m horizon
        df = await _fetch_df(venue, symbol, cfg.interval, max(cfg.lookback, 180))
        if len(df) == 0:
            continue
        for s in items:
            ts_ms = int(datetime.fromisoformat(s["ts"].replace("Z","+00:00")).timestamp() * 1000)
            i0 = _nearest_index(df, ts_ms)
            if i0 < 0 or i0 >= len(df):
                continue
            entry = float(df.iloc[i0]["close"])
            for h in HORIZONS_MIN:
                bars = h // (1 if cfg.interval.endswith("m") else 60)  # assuming 1m or 1h intervals
                i1 = min(i0 + max(bars,1), len(df)-1)
                ret, max_fav, max_adv, exit_price = _calc_window_metrics(df, i0, i1, s["side"], entry)
                rows.append({
                    "signal_id": s["id"],
                    "ts": s["ts"],
                    "venue": venue,
                    "symbol": symbol,
                    "side": s["side"],
                    "horizon_m": h,
                    "entry_price": entry,
                    "exit_price": exit_price,
                    "ret": ret,
                    "max_fav": max_fav,
                    "max_adv": max_adv,
                })

    if rows:
        await supa.upsert("signal_outcomes", rows, on_conflict="signal_id,horizon_m")
        log.info(f"Computed outcomes: {len(rows)} rows")
    else:
        log.info("No outcome rows to upsert.")

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(compute_outcomes())
