# app/policy.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

# Bucketing helpers
EVENT_GOOD = ("Perp Discount Capitulation", "Perp Premium Blowoff")
RSI_FAMILY = ("RSI Reversal", "RSI Reversal Risk", "Premium", "Discount")
MOMENTUM   = ("Momentum Pop",)

@dataclass
class _Last:
    ts: float
    side: str
    score: float

class _Cooldowns:
    """
    Tracks last trade per (bucket,symbol) with anti-flip and min spacing.
    """
    def __init__(self):
        self._mem: Dict[str, _Last] = {}

    def ok(self, key: str, *, min_gap_s: int, new_side: str, new_score: float, flip_bonus: float) -> bool:
        now = time.time()
        last = self._mem.get(key)
        if last is None:
            self._mem[key] = _Last(now, new_side, new_score)
            return True

        # 1) spacing
        if (now - last.ts) < min_gap_s:
            return False

        # 2) anti flip-flop: if changing side, require >= previous score + flip_bonus
        if last.side != new_side and (new_score < last.score + flip_bonus):
            return False

        self._mem[key] = _Last(now, new_side, new_score)
        return True


class TradingPolicy:
    """
    Central trade filter. Keep it conservative; the scoring function handles ‘alpha’,
    this policy handles *when to act*.
    """

    # score gates (tune here)
    MIN_EVENT = 5.5      # Perp capitulation/blowoff style
    MIN_RSI   = 6.0      # mean-reversion / premium/discount + RSI combos
    MIN_MOMO  = 4.2      # momentum pops (executor can size smaller)

    # cooldowns (seconds)
    CD_EVENT = 180       # symbol-level spacing for event plays
    CD_RSI   = 600       # RSI/premium/discount style
    CD_MOMO  = 900       # momentum pops (avoid spam)

    # flipping protection
    FLIP_BONUS = 0.75    # if flipping side, require new_score >= old_score + FLIP_BONUS

    def __init__(self):
        self._cool = _Cooldowns()

    @staticmethod
    def _bucket(reason: Optional[str], triggers: Optional[Iterable[str]]) -> str:
        r = (reason or "").strip()
        t = " ".join(list(triggers or []))

        blob = f"{r} {t}"

        if any(s in blob for s in EVENT_GOOD):
            return "event"
        if any(s in blob for s in MOMENTUM):
            return "momo"
        if any(s in blob for s in RSI_FAMILY):
            return "rsi"
        return "other"

    def should_trade(
        self,
        *,
        signal_type: str,
        side: str,
        score: float,
        rsi: Optional[float],
        vwap: Optional[float],
        close: Optional[float],
        triggers: Optional[Iterable[str]],
        symbol: Optional[str] = None,
        reason: Optional[str] = None,
        venue: Optional[str] = None,            # accepted (unused now; useful later)
        **_ignore,                               # swallow any future keywords safely
    ) -> bool:
        """
        Return True if we should place a trade for this signal.
        """
        sym   = (symbol or "").upper()
        side  = (side or "").upper()
        score = float(score or 0.0)

        bucket = self._bucket(reason, triggers)

        # Event plays
        if bucket == "event":
            if score < self.MIN_EVENT:
                return False
            key = f"event::{sym}"
            return self._cool.ok(key, min_gap_s=self.CD_EVENT, new_side=side,
                                 new_score=score, flip_bonus=self.FLIP_BONUS)

        # RSI / premium/discount mean-reversion style
        if bucket == "rsi":
            if score < self.MIN_RSI:
                return False
            key = f"rsi::{sym}"
            return self._cool.ok(key, min_gap_s=self.CD_RSI, new_side=side,
                                 new_score=score, flip_bonus=self.FLIP_BONUS)

        # Momentum pops
        if bucket == "momo":
            if score < self.MIN_MOMO:
                return False
            key = f"momo::{sym}"
            return self._cool.ok(key, min_gap_s=self.CD_MOMO, new_side=side,
                                 new_score=score, flip_bonus=self.FLIP_BONUS)

        # everything else: skip
        return False


# Singleton instance imported by main
POLICY = TradingPolicy()
