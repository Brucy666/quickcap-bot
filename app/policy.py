# app/policy.py
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Dict, Tuple

EVENT_GOOD = ("Perp Discount Capitulation", "Perp Premium Blowoff")
RSI_FAMILY  = ("RSI Reversal", "RSI Reversal Risk")
MOMENTUM    = ("Momentum Pop",)

@dataclass
class Decision:
    take: bool
    why: str
    cooldown_s: int = 0

class _CooldownCache:
    def __init__(self):
        self.last: Dict[str, Tuple[float, str, float]] = {}  # key -> (ts, side, score)

    def ok(self, key: str, min_gap_s: int, new_side: str, new_score: float, flip_bonus: float = 0.75) -> bool:
        now = time.time()
        if key not in self.last:
            self.last[key] = (now, new_side, new_score)
            return True
        ts, prev_side, prev_score = self.last[key]
        # 1) basic cooldown
        if now - ts < min_gap_s:
            return False
        # 2) anti flip-flop: if changing side, require strictly higher score improvement
        if prev_side != new_side and (new_score < prev_score + flip_bonus):
            return False
        self.last[key] = (now, new_side, new_score)
        return True

COOLDOWNS = _CooldownCache()

@dataclass
class Policy:
    # thresholds
    min_score_event: float = 5.5     # event plays
    min_score_rsi: float   = 6.0     # rsi/premium-discount mean reversion
    min_score_momo: float  = 4.2     # momentum pop (de-risked smaller size in executor)

    # cooldowns
    cd_event_s: int = 180            # 3 min on same symbol for event plays
    cd_rsi_s: int   = 600            # 10 min on same symbol for rsi style
    cd_momo_s: int  = 900            # 15 min on momentum pops (no spam)

    # other guards
    flip_bonus: float = 0.75         # if flipping side, require new score >= old + this

    def _reason_bucket(self, reason: str) -> str:
        r = reason or ""
        if any(t in r for t in EVENT_GOOD):   return "event"
        if any(t in r for t in MOMENTUM):     return "momo"
        if "Premium" in r or "Discount" in r or any(t in r for t in RSI_FAMILY):
            return "rsi"
        return "other"

    def should_trade(self, sig: dict) -> Decision:
        """
        sig expects: symbol, side, score, reason (str), ts (iso or epoch ok)
        """
        sym   = sig.get("symbol","").upper()
        side  = (sig.get("side","") or "").upper()
        score = float(sig.get("score") or 0.0)
        reason= str(sig.get("reason") or " ".join(sig.get("triggers",[]) or []))

        bucket = self._reason_bucket(reason)

        if bucket == "event":
            if score < self.min_score_event:
                return Decision(False, f"score<{self.min_score_event} for EVENT")
            key = f"event::{sym}"
            if not COOLDOWNS.ok(key, self.cd_event_s, side, score, self.flip_bonus):
                return Decision(False, "event cooldown / flip gate")
            return Decision(True, "EVENT ok", self.cd_event_s)

        if bucket == "rsi":
            if score < self.min_score_rsi:
                return Decision(False, f"score<{self.min_score_rsi} for RSI")
            key = f"rsi::{sym}"
            if not COOLDOWNS.ok(key, self.cd_rsi_s, side, score, self.flip_bonus):
                return Decision(False, "rsi cooldown / flip gate")
            return Decision(True, "RSI ok", self.cd_rsi_s)

        if bucket == "momo":
            if score < self.min_score_momo:
                return Decision(False, f"score<{self.min_score_momo} for MOMO")
            key = f"momo::{sym}"
            if not COOLDOWNS.ok(key, self.cd_momo_s, side, score, self.flip_bonus):
                return Decision(False, "momo cooldown / flip gate")
            return Decision(True, "MOMO ok (reduced size)", self.cd_momo_s)

        # default: ignore anything else
        return Decision(False, "unknown bucket")
