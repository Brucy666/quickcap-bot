# app/policy.py
from __future__ import annotations
import os, time
from dataclasses import dataclass
from typing import Dict, Tuple, Iterable, Optional

# ---- Buckets by reason text ----
EVENT_GOOD = ("Perp Discount Capitulation", "Perp Premium Blowoff")
RSI_FAMILY  = ("RSI Reversal", "RSI Reversal Risk", "Discount + RSI", "Premium + RSI")
MOMENTUM    = ("Momentum Pop",)

# ---------- cooldown / anti flip-flop ----------
class _CooldownCache:
    def __init__(self):
        # key -> (last_ts, last_side, last_score)
        self.last: Dict[str, Tuple[float, str, float]] = {}

    def ok(self, key: str, min_gap_s: int, new_side: str, new_score: float, flip_bonus: float) -> bool:
        now = time.time()
        prev = self.last.get(key)
        if prev is None:
            self.last[key] = (now, new_side, new_score)
            return True
        ts, prev_side, prev_score = prev

        # 1) basic cooldown
        if now - ts < min_gap_s:
            return False

        # 2) anti flip-flop: if we are changing side, require strictly better score
        if prev_side != new_side and (new_score < prev_score + flip_bonus):
            return False

        self.last[key] = (now, new_side, new_score)
        return True

COOLDOWNS = _CooldownCache()

# -------------- helpers --------------
def _f(env: str, default: float) -> float:
    try: return float(os.getenv(env, default))
    except Exception: return default

def _b(env: str, default: bool) -> bool:
    v = os.getenv(env)
    return default if v is None else v.strip() in ("1", "true", "True", "yes", "YES")

def _reason_bucket(reason: str) -> str:
    r = reason or ""
    if any(t in r for t in EVENT_GOOD): return "event"
    if any(t in r for t in MOMENTUM):   return "momo"
    if "Premium" in r or "Discount" in r or any(t in r for t in RSI_FAMILY):
        return "rsi"
    return "other"

def _has(trigs: Optional[Iterable[str]], needle: str) -> bool:
    if not trigs: return False
    n = needle.lower()
    return any(n in str(x).lower() for x in trigs)

# -------------- decision type --------------
@dataclass
class Decision:
    take: bool
    why: str
    cooldown_s: int = 0

# -------------- main policy --------------
@dataclass
class TradingPolicy:
    # thresholds (env overrideable)
    min_score_event: float = _f("POLICY_MIN_SCORE_EVENT", 5.5)
    min_score_rsi:   float = _f("POLICY_MIN_SCORE_RSI",   6.0)
    min_score_momo:  float = _f("POLICY_MIN_SCORE_MOMO",  4.2)

    # cool-downs (seconds)
    cd_event_s: int = int(_f("POLICY_CD_EVENT_S", 180))   # 3 min
    cd_rsi_s:   int = int(_f("POLICY_CD_RSI_S",   600))   # 10 min
    cd_momo_s:  int = int(_f("POLICY_CD_MOMO_S",  900))   # 15 min

    # anti flip-flop bonus: new score must exceed previous by this much when changing side
    flip_bonus: float = _f("POLICY_FLIP_BONUS", 0.75)

    # quality rails
    rsi_long_max: float = _f("POLICY_RSI_LONG_MAX", 72.0)
    rsi_short_min: float = _f("POLICY_RSI_SHORT_MIN", 28.0)
    max_vwap_dist_pct: float = _f("POLICY_MAX_VWAP_DIST_PCT", 0.35)

    # toggles
    allow_momentum: bool = _b("POLICY_ALLOW_MOMENTUM", False)

    # ---- Public API (the one live worker uses) ----
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
    ) -> bool:
        """
        Central gate for live trades. Returns True to allow the trade.
        Also enforces per-bucket cooldown + anti-flip logic using symbol.
        """
        sym   = (symbol or "").upper()
        sd    = (side or "").upper()
        sc    = float(score or 0.0)
        rs    = None if rsi is None else float(rsi)
        vw    = None if vwap is None else float(vwap)
        cl    = None if close is None else float(close)

        # Build a reason string if not provided (used for bucketing)
        if not reason:
            reason = ", ".join(triggers or []) if triggers else ""

        # Momentum optionally disabled
        if _has(triggers, "Momentum Pop") and not self.allow_momentum:
            return False

        # Quality rails (spot & basis): near VWAP and sane RSI
        if vw and cl:
            dist = abs(cl - vw) / vw * 100.0
            if dist > self.max_vwap_dist_pct:
                return False
        if signal_type != "basis" and rs is not None:  # RSI rails only for spot
            if sd == "LONG"  and rs > self.rsi_long_max:  return False
            if sd == "SHORT" and rs < self.rsi_short_min: return False

        # Bucket & thresholds
        bucket = _reason_bucket(reason)
        if bucket == "event":
            if sc < self.min_score_event: return False
            key = f"event::{sym}"
            return COOLDOWNS.ok(key, self.cd_event_s, sd, sc, self.flip_bonus)

        if bucket == "rsi":
            if sc < self.min_score_rsi: return False
            key = f"rsi::{sym}"
            return COOLDOWNS.ok(key, self.cd_rsi_s, sd, sc, self.flip_bonus)

        if bucket == "momo":
            if sc < self.min_score_momo: return False
            key = f"momo::{sym}"
            return COOLDOWNS.ok(key, self.cd_momo_s, sd, sc, self.flip_bonus)

        # Ignore anything we can't classify
        return False

    # ---- Backwards-compat: old dict style ----
    def should_trade_dict(self, sig: Dict) -> Decision:
        """
        Legacy entrypoint used by backfill/tests.
        Expects keys: symbol, side, score, reason (or triggers), ts...
        """
        symbol = (sig.get("symbol") or "").upper()
        side   = (sig.get("side") or "").upper()
        score  = float(sig.get("score") or 0.0)
        reason = str(sig.get("reason") or " ".join(sig.get("triggers", []) or []))

        # quick bucket
        bucket = _reason_bucket(reason)
        if bucket == "event":
            if score < self.min_score_event:
                return Decision(False, f"score<{self.min_score_event} for EVENT")
            ok = COOLDOWNS.ok(f"event::{symbol}", self.cd_event_s, side, score, self.flip_bonus)
            return Decision(ok, "EVENT ok" if ok else "event cooldown/flip", self.cd_event_s)

        if bucket == "rsi":
            if score < self.min_score_rsi:
                return Decision(False, f"score<{self.min_score_rsi} for RSI")
            ok = COOLDOWNS.ok(f"rsi::{symbol}", self.cd_rsi_s, side, score, self.flip_bonus)
            return Decision(ok, "RSI ok" if ok else "rsi cooldown/flip", self.cd_rsi_s)

        if bucket == "momo":
            if score < self.min_score_momo:
                return Decision(False, f"score<{self.min_score_momo} for MOMO")
            ok = COOLDOWNS.ok(f"momo::{symbol}", self.cd_momo_s, side, score, self.flip_bonus)
            return Decision(ok, "MOMO ok" if ok else "momo cooldown/flip", self.cd_momo_s)

        return Decision(False, "unknown bucket", 0)


# Exported singleton used by app.main
POLICY = TradingPolicy()
