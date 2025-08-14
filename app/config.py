# app/config.py
from pydantic import BaseModel, Field
import os

# --------- Safe defaults ---------
DEFAULTS = {
    "EXCHANGES": "kucoin",
    "SYMBOLS": "BTCUSDT,ETHUSDT",
    "INTERVAL": "1m",                 # allowed: 1m,3m,5m,15m,1h
    "LOOKBACK_CANDLES": "500",
    "DISCORD_WEBHOOK": "",
    "RISK_OFF": "false",
    "ALERT_COOLDOWN_SEC": "180",
    "SCAN_PERIOD_SEC": "60",
    "ALERT_MIN_SCORE": "2.0",
    "MOMENTUM_Z": "2.0",
    "MAX_POSITION_PER_SYMBOL_USDT": "200",
    "KUCOIN_API_KEY": "",
    "KUCOIN_API_SECRET": "",
    "KUCOIN_API_PASSPHRASE": "",
    "MEXC_API_KEY": "",
    "MEXC_API_SECRET": "",
}
_VALID_INTERVALS = {"1m", "3m", "5m", "15m", "1h"}

# --------- helpers (sanitize + parse) ---------
def _raw_env(key: str) -> str | None:
    v = os.getenv(key)
    return v if v is not None and v != "" else None

def _sanitize(v: str) -> str:
    s = v.strip()
    # remove surrounding quotes if present
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1].strip()
    # drop stray backslashes
    s = s.replace("\\", "").strip()
    return s

def _get(key: str) -> str:
    v = _raw_env(key)
    return _sanitize(v) if v is not None else DEFAULTS[key]

def _split_csv(v: str) -> list[str]:
    s = _sanitize(v)
    return [p.strip() for p in s.split(",") if p.strip()]

def _to_bool(v: str) -> bool:
    return _sanitize(v).lower() in {"1", "true", "yes", "y", "on"}

def _to_int(v: str, default: int) -> int:
    try:
        return int(_sanitize(v))
    except Exception:
        return int(default)

def _to_float(v: str, default: float) -> float:
    try:
        return float(_sanitize(v))
    except Exception:
        return float(default)

# --------- Settings ---------
class Settings(BaseModel):
    # Core
    exchanges: list[str] = Field(default_factory=lambda: DEFAULTS["EXCHANGES"].split(","))
    symbols: list[str]   = Field(default_factory=lambda: [s.strip() for s in DEFAULTS["SYMBOLS"].split(",")])
    interval: str = DEFAULTS["INTERVAL"]
    lookback: int = int(DEFAULTS["LOOKBACK_CANDLES"])

    # Alert & control
    discord_webhook: str | None = DEFAULTS["DISCORD_WEBHOOK"] or None
    risk_off: bool = False
    alert_cooldown_sec: int = int(DEFAULTS["ALERT_COOLDOWN_SEC"])
    scan_period_sec: int = int(DEFAULTS["SCAN_PERIOD_SEC"])
    alert_min_score: float = float(DEFAULTS["ALERT_MIN_SCORE"])
    momentum_z: float = float(DEFAULTS["MOMENTUM_Z"])

    # Sizing
    max_pos_usdt: float = float(DEFAULTS["MAX_POSITION_PER_SYMBOL_USDT"])

    # Keys (optional for scanning)
    kucoin_key: str | None = None
    kucoin_secret: str | None = None
    kucoin_passphrase: str | None = None
    mexc_key: str | None = None
    mexc_secret: str | None = None

    def validate_interval(self):
        if self.interval not in _VALID_INTERVALS:
            raise SystemExit(f"[CONFIG ERROR] INTERVAL must be one of {_VALID_INTERVALS}, got '{self.interval}'")

def load_settings() -> Settings:
    s = Settings(
        exchanges=_split_csv(_get("EXCHANGES")),
        symbols=_split_csv(_get("SYMBOLS")),
        interval=_get("INTERVAL"),
        lookback=_to_int(_get("LOOKBACK_CANDLES"), int(DEFAULTS["LOOKBACK_CANDLES"])),
        discord_webhook=_get("DISCORD_WEBHOOK") or None,
        risk_off=_to_bool(_get("RISK_OFF")),
        alert_cooldown_sec=_to_int(_get("ALERT_COOLDOWN_SEC"), int(DEFAULTS["ALERT_COOLDOWN_SEC"])),
        scan_period_sec=_to_int(_get("SCAN_PERIOD_SEC"), int(DEFAULTS["SCAN_PERIOD_SEC"])),
        alert_min_score=_to_float(_get("ALERT_MIN_SCORE"), float(DEFAULTS["ALERT_MIN_SCORE"])),
        momentum_z=_to_float(_get("MOMENTUM_Z"), float(DEFAULTS["MOMENTUM_Z"])),
        max_pos_usdt=_to_float(_get("MAX_POSITION_PER_SYMBOL_USDT"), float(DEFAULTS["MAX_POSITION_PER_SYMBOL_USDT"])),
        kucoin_key=_get("KUCOIN_API_KEY") or None,
        kucoin_secret=_get("KUCOIN_API_SECRET") or None,
        kucoin_passphrase=_get("KUCOIN_API_PASSPHRASE") or None,
        mexc_key=_get("MEXC_API_KEY") or None,
        mexc_secret=_get("MEXC_API_SECRET") or None,
    )
    s.validate_interval()
    return s
