import asyncio
from typing import Iterable, Dict, List
from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BinanceSpotPublic, OKXSpotPublic, BybitSpotPublic,
)

async def _safe(call):
    try:
        return await call
    except Exception:
        return []

async def build_hotmap(
    exchanges: Iterable[str],
    top_n: int = 20,
    min_vol_usdt: float = 0.0,
    force_symbols: Iterable[str] = (),
    exclude_symbols: Iterable[str] = (),
) -> Dict[str, List[str]]:
    """
    Returns per-venue hot symbols: { 'kucoin': [...], 'binance': [...], ... }
    Symbols are venue-native and already tradable on that venue.
    """
    ex = {x.strip().lower() for x in exchanges if x.strip()}
    tasks = {}
    if "kucoin" in ex:  tasks["kucoin"]  = _safe(KuCoinPublic.top_symbols(top_n, min_vol_usdt))
    if "mexc" in ex:    tasks["mexc"]    = _safe(MEXCPublic.top_symbols(top_n, min_vol_usdt))
    if "binance" in ex: tasks["binance"] = _safe(BinanceSpotPublic.top_symbols(top_n, min_vol_usdt))
    if "okx" in ex:     tasks["okx"]     = _safe(OKXSpotPublic.top_symbols(top_n, min_vol_usdt))
    if "bybit" in ex:   tasks["bybit"]   = _safe(BybitSpotPublic.top_symbols(top_n, min_vol_usdt))

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    hot: Dict[str, List[str]] = {}
    for k, res in zip(tasks.keys(), results):
        syms = [] if isinstance(res, Exception) else list(dict.fromkeys(res))  # uniq + keep order
        hot[k] = syms

    # apply excludes per-venue; add force symbols if they exist on the venue (BTC/ETH safe everywhere)
    excludes = {s.strip() for s in exclude_symbols if s and s.strip()}
    forced   = [s.strip() for s in force_symbols if s and s.strip()]
    for venue, syms in hot.items():
        syms = [s for s in syms if s not in excludes]
        # ensure forced symbols appear first if present on the venue (BTC/ETH are universal)
        for f in forced:
            if f in syms:
                syms.remove(f)
                syms.insert(0, f)
            else:
                # if not present, keep as-is (do not inject to avoid invalid-symbol errors)
                pass
        hot[venue] = syms[:top_n]

    return hot
