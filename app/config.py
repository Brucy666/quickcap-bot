from pydantic import BaseModel, Field, ValidationError
import os

_VALID_INTERVALS = {"1m", "3m", "5m", "15m", "1h"}

class Settings(BaseModel):
    # Core
    exchanges: list[str] = Field(default_factory=lambda: ["kucoin"])
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT"])
    interval: str = "1m"
    lookback: int = 500

    # Alerting / controls
    discord_webhook: str | None = None
    risk_off: bool = False
    alert_cooldown_sec: int = 120
    scan_period_sec: int = 60
    alert_min_score: float = 0.0
    momentum_z: float = 2.0  # sensitivity for momentum pop (used by signals)

    # Paper/live (live executor can read these later)
    max_pos_usdt: float = 200.0

    # Exchange keys (optional for scanning)
    kucoin_key: str | None = None
    kucoin_secret: str | None = None
    kucoin_passphrase: str | None = None

    mexc_key: str | None = None
    mexc_secret: str | None = None

    def validate_interval(self):
        if self.interval not in _VALID_INTERVALS:
            raise ValueError(f"INTERVAL must be one of {_VALID_INTERVALS}, got '{self.interval}'")

def _split_csv(env_key: str, default_csv: str) -> list[str]:
    raw = os.getenv(env_key, default_csv)
    return [x.strip() for x in raw.split(",") if x.strip()]

def _get_bool(env_key: str, default: bool) -> bool:
    return os.getenv(env_key, str(default)).strip().lower() in {"1","true","yes","y","on"}

def load_settings() -> Settings:
    try:
        s = Settings(
            exchanges=_split_csv("EXCHANGES", "kucoin"),
            symbols=_split_csv("SYMBOLS", "BTCUSDT"),
            interval=os.getenv("INTERVAL", "1m"),
            lookback=int(os.getenv("LOOKBACK_CANDLES", "500")),
            discord_webhook=os.getenv("DISCORD_WEBHOOK"),
            risk_off=_get_bool("RISK_OFF", False),
            alert_cooldown_sec=int(os.getenv("ALERT_COOLDOWN_SEC", "120")),
            scan_period_sec=int(os.getenv("SCAN_PERIOD_SEC", "60")),
            alert_min_score=float(os.getenv("ALERT_MIN_SCORE", "0.0")),
            momentum_z=float(os.getenv("MOMENTUM_Z", "2.0")),
            max_pos_usdt=float(os.getenv("MAX_POSITION_PER_SYMBOL_USDT", "200")),
            kucoin_key=os.getenv("KUCOIN_API_KEY"),
            kucoin_secret=os.getenv("KUCOIN_API_SECRET"),
            kucoin_passphrase=os.getenv("KUCOIN_API_PASSPHRASE"),
            mexc_key=os.getenv("MEXC_API_KEY"),
            mexc_secret=os.getenv("MEXC_API_SECRET"),
        )
        s.validate_interval()
        return s
    except (ValidationError, ValueError) as e:
        # Fail fast with clear message (Railway logs will show this)
        raise SystemExit(f"[CONFIG ERROR] {e}")
