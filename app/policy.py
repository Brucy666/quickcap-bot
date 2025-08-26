# app/policy.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Tuple

# Buckets we key off in the reason / triggers text
EVENT_GOOD = ("Perp Discount Capitulation", "Perp Premium Blowoff")
RSI_FAMILY = ("RSI Reversal", "RSI Reversal Risk", "Premium", "Discount")
MOMENTUM   = ("Momentum Pop",)

@dataclass
class Decision:
    take: bool
    why: str
    cooldown_s: int = 0

class _CooldownCache:
    """
    Per-key cooldown with an anti flip-flop gate (if changing side, require a better score).
    key -> (last_ts, last_side, last_score)
    """
    def __init__(self):
        self.last: Dict[str, Tuple[float, str, float]] = {}

    def ok(self, key: str, min_gap_s: int, new_side: str, new_score: float, flip_bonus: float = 0.75) -> bool:
        now = time.time()
        if key not in self.last:
            self.last[key] = (now, new_side, new_score)
            return True
        ts, prev_side, prev_score = self.last[key]
        if now - ts < min_gap_s:
            return False
        if prev_side != new_side and (new_score < prev_score + flip_bonus):
            return False
        self.last[key] = (now, new_side, new_score)
        return True

COOLDOWNS = _CooldownCache()

@dataclass
class TradingPolicy:
    # thresholds
    min_score_event: float = 5.5
    min_score_rsi:   float = 6.0
    min_score_momo:  float = 4.2

    # cooldowns (per symbol per bucket)
    cd_event_s: int = 180     # 3 min
    cd_rsi_s:   int = 600     # 10 min
    cd_momo_s:  int = 900     # 15 min

    # flip guard
    flip_bonus: float = 0.75

    def _bucket(self, reason: str, triggers: list[str] | None) -> str:
        text = (reason or "") + " " + " ".join(triggers or [])
        if any(t in text for t in EVENT_GOOD): return "event"
        if any(t in text for t in MOMENTUM):   return "momo"
        if any(t in text for t in RSI_FAMILY): return "rsi"
        return "other"

    def should_trade(self, **sig) -> Decision:
        """
        Call with keyword arguments, e.g. POLICY.should_trade(**signal_row)

        Expected keys (robust to missing):
        symbol, side, score, reason, triggers, signal_type, rsi, vwap, close, ts, venue, interval
        """
        sym   = str(sig.get("symbol","")).upper()
        side  = str(sig.get("side","")).upper()
        score = float(sig.get("score") or 0.0)

        reason   = str(sig.get("reason") or "")
        triggers = list(sig.get("triggers") or [])
        bucket   = self._bucket(reason, triggers)

        if bucket == "event":
            if score < self.min_score_event:
                return Decision(False, f"score<{self.min_score_event} EVENT")
            key = f"event::{sym}"
            if not COOLDOWNS.ok(key, self.cd_event_s, side, score, self.flip_bonus):
                return Decision(False, "EVENT cooldown/flip gate")
            return Decision(True, "EVENT ok", self.cd_event_s)

        if bucket == "rsi":
            if score < self.min_score_rsi:
                return Decision(False, f"score<{self.min_score_rsi} RSI")
            key = f"rsi::{sym}"
            if not COOLDOWNS.ok(key, self.cd_rsi_s, side, score, self.flip_bonus):
                return Decision(False, "RSI cooldown/flip gate")
            return Decision(True, "RSI ok", self.cd_rsi_s)

        if bucket == "momo":
            if score < self.min_score_momo:
                return Decision(False, f"score<{self.min_score_momo} MOMO")
            key = f"momo::{sym}"
            if not COOLDOWNS.ok(key, self.cd_momo_s, side, score, self.flip_bonus):
                return Decision(False, "MOMO cooldown/flip gate")
            return Decision(True, "MOMO ok (reduced size)", self.cd_momo_s)

        return Decision(False, "unknown bucket")

# Singleton used everywhere
POLICY = TradingPolicy()
