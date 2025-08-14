import asyncio
from typing import Iterable, Set, List
from app.exchanges import (
    KuCoinPublic, MEXCPublic,
    BybitSpotPublic, BinanceSpotPublic, OKXSpotPublic
)

async def _kucoin_hot(top_n: int, min_vol_usdt: float) -> List[str]:
    try:
        return await KuCoinPublic.top_symbols(top_n=top_n, min_vol_usdt=min_vol_usdt)
    except Exception:
        return []

async def _mexc_hot(top_n: int, min_vol_usdt: float) -> List[str]:
    try:
        return await MEXCPublic.top_symbols(top_n=top_n, min_vol_usdt=min_vol_usdt)
    except Exception:
        return []

async def _bybit_hot(top_n: int, min_vol_usdt: float) -> List[str]:
    try:
        return await BybitSpotPublic.top_symbols(top_n=top_n, min_vol_usdt=min_vol_usdt)
    except Exception:
        return []

async def _binance_hot(top_n: int, min_vol_usdt: float) -> List[str]:
    try:
        return await BinanceSpotPublic.top_symbols(top_n=top_n, min_vol_usdt=min_vol_usdt)
    except Exception:
        return []

async def _okx_hot(top_n: int, min_vol_usdt: float) -> List[str]:
    try:
        return await OKXSpotPublic.top_symbols(top_n=top_n, min_vol_usdt=min_vol_usdt)
    except Exception:
        return []

async def build_hotlist(
    exchanges: Iterable[str],
    top_n: int = 20,
    min_vol_usdt: float = 0.0,
    force_symbols: Iterable[str] = (),
    exclude_symbols: Iterable[str] = (),
) -> List[str]:
    """
    Build a deduped symbol list by pulling top movers/volume from enabled venues.
    Returns up to top_n unique symbols (plus forced), excluding any in exclude_symbols.
    """
    ex = {x.strip().lower() for x in exchanges if x.strip()}
    tasks = []
    if "kucoin" in ex:  tasks.append(_kucoin_hot(top_n, min_vol_usdt))
    if "mexc" in ex:    tasks.append(_mexc_hot(top_n, min_vol_usdt))
    if "bybit" in ex:   tasks.append(_bybit_hot(top_n, min_vol_usdt))
    if "binance" in ex: tasks.append(_binance_hot(top_n, min_vol_usdt))
    if "okx" in ex:     tasks.append(_okx_hot(top_n, min_vol_usdt))

    out: Set[str] = set()
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                continue
            out.update(res)

    out.update([s.strip() for s in force_symbols if s and s.strip()])
    out.difference_update([s.strip() for s in exclude_symbols if s and s.strip()])

    # Stable, trimmed list
    return sorted(out)[: top_n]
