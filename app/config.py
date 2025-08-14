# app/config.py
from pydantic import BaseModel, Field, ValidationError
import os

# --------- Hard defaults (works even with no env set) ---------
DEFAULTS = {
    "EXCHANGES": "kucoin",
    "SYMBOLS": "BTCUSDT,ETHUSDT",
    "INTERVAL": "1m",                 # allowed: 1m,3m,5m,15m,1h
    "LOOKBACK_CANDLES": "500",
    "DISCORD_WEBHOOK": "",            # optional; logs if empty
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
            raise ValueError(f"INTERVAL must be one of {_VALID_INTERVALS}, got '{self.interval}'")

def _env(key: str) -> str:
    val = os.getenv(key)
    return val if val is not None and val != "" else DEFAULTS[key]

def _split_csv(val: str) -> list[str]:
    return [x.strip() for x in val.split(",") if x.strip()]

def _to_bool(val: str) -> bool:
    return val.strip().lower() in {"1","true","yes","y","on"}

def load_settings() -> Settings:
    try:
        s = Settings(
            exchanges=_split_csv(_env("EXCHANGES")),
            symbols=_split_csv(_env("SYMBOLS")),
            interval=_env("INTERVAL"),
            lookback=int(_env("LOOKBACK_CANDLES")),
            discord_webhook=_env("DISCORD_WEBHOOK") or None,
            risk_off=_to_bool(_env("RISK_OFF")),
            alert_cooldown_sec=int(_env("ALERT_COOLDOWN_SEC")),
            scan_period_sec=int(_env("SCAN_PERIOD_SEC")),
            alert_min_score=float(_env("ALERT_MIN_SCORE")),
            momentum_z=float(_env("MOMENTUM_Z")),
            max_pos_usdt=float(_env("MAX_POSITION_PER_SYMBOL_USDT")),
            kucoin_key=_env("KUCOIN_API_KEY") or None,
            kucoin_secret=_env("KUCOIN_API_SECRET") or None,
            kucoin_passphrase=_env("KUCOIN_API_PASSPHRASE") or None,
            mexc_key=_env("MEXC_API_KEY") or None,
            mexc_secret=_env("MEXC_API_SECRET") or None,
        )
        s.validate_interval()
        return s
    except (ValidationError, ValueError) as e:
        raise SystemExit(f"[CONFIG ERROR] {e}")
