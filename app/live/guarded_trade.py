# app/live/guarded_trade.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from app.policy import Policy

try:
    # Optional: only used if you already wired Discord
    from app.notifier import post_signal_embed
except Exception:  # pragma: no cover
    post_signal_embed = None  # type: ignore

POLICY = Policy()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _join_triggers(trigs: Optional[Iterable[str]]) -> str:
    if not trigs:
        return ""
    return ", ".join([str(t) for t in trigs if str(t).strip()])

@dataclass
class TradeResult:
    taken: bool
    why: str

async def maybe_trade(
    sig_row: Dict[str, Any],
    *,
    executor: Any,                    # your live executor (async submit)
    supa: Optional[Any] = None,       # Supabase client (optional)
    venue: str = "binance",
    interval: str = "1m",
    size_by_bucket: bool = True,
) -> TradeResult:
    """
    Gate a live trade by the Policy. If accepted:
      - (optionally) log to Supabase
      - (optionally) post to Discord
      - execute via executor.submit(...)

    Expected sig_row keys (best-effort; extras are fine):
      symbol, side, price, score, triggers(list[str]) or reason(str),
      vwap?, rsi?, ts? (iso string)
    """
    symbol = str(sig_row.get("symbol"))
    side   = str(sig_row.get("side", "")).upper()
    score  = float(sig_row.get("score", 0.0))
    price  = float(sig_row.get("price", 0.0))
    reason = sig_row.get("reason") or _join_triggers(sig_row.get("triggers"))
    ts     = str(sig_row.get("ts") or _now_iso())

    # --- Policy gate ---
    decision = POLICY.should_trade({
        "symbol": symbol,
        "side":   side,
        "score":  score,
        "reason": reason,
        "ts":     ts,
    })
    if not decision.take:
        print(f"[POLICY] SKIP {symbol} {side} score={score:.2f} reason='{reason}' -> {decision.why}")
        return TradeResult(False, decision.why)

    # --- Optional: position sizing by bucket ---
    size_mult = 1.0
    if size_by_bucket:
        bucket = decision.why.lower()
        if "event" in bucket:        # strong: premium+div+cooldown ok
            size_mult = 1.00
        elif "rsi" in bucket:        # medium: RSI confluence / near vwap
            size_mult = 0.60
        elif "momo" in bucket:       # weakest: raw momentum pop
            size_mult = 0.35

    # --- Optional: log signal to Supabase (idempotent on your side) ---
    if supa is not None:
        try:
            # Keep whatever schema you already use — pass through keys
            await supa.log_signal(**{
                **sig_row,
                "signal_type": sig_row.get("signal_type", "spot"),
                "venue":       sig_row.get("venue", venue),
                "interval":    sig_row.get("interval", interval),
                "reason":      reason,
                "ts":          ts,
            })
        except Exception as e:  # don't block execution on logging issues
            print(f"[LIVE] supa.log_signal failed: {e}")

    # --- Optional: Discord embed if your notifier is present ---
    if post_signal_embed is not None:
        try:
            await post_signal_embed(
                exchange=venue,
                symbol=symbol,
                side=side,
                price=price,
                score=score,
                triggers=[t.strip() for t in (reason.split(",") if reason else []) if t.strip()],
                interval=interval,
            )
        except Exception as e:
            print(f"[LIVE] discord post failed: {e}")

    # --- Execute the trade (be liberal with executor signatures) ---
    try:
        # Preferred signature with sizing + reason
        await executor.submit(symbol, side, price, score, reason, size_mult=size_mult)
    except TypeError:
        try:
            # Common signature with reason only
            await executor.submit(symbol, side, price, score, reason)
        except TypeError:
            # Minimal fall-back
            await executor.submit(symbol, side, price, score)

    print(f"[POLICY] TAKE  {symbol} {side} score={score:.2f} size_mult={size_mult:.2f} reason='{reason}'")
    return TradeResult(True, decision.why)
