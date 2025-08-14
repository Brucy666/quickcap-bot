from pydantic import BaseModel, Field
import os

DEFAULTS = {
    # Core scan
    "EXCHANGES": "kucoin,binance,okx,bybit,mexc",
    "SYMBOLS": "BTCUSDT,ETHUSDT",
    "INTERVAL": "1m",
    "LOOKBACK_CANDLES": "500",

    # Alerts / risk
    "DISCORD_WEBHOOK": "",
    "RISK_OFF": "false",
    "ALERT_COOLDOWN_SEC": "180",
    "SCAN_PERIOD_SEC": "60",
    "ALERT_MIN_SCORE": "2.3",
    "MOMENTUM_Z": "2.0",
    "MAX_POSITION_PER_SYMBOL_USDT": "200",

    # Hotlist
    "HOTLIST_ENABLED": "true",
    "HOTLIST_TOP_N": "20",
    "HOTLIST_MIN_VOL_USDT": "200000",
    "FORCE_SYMBOLS": "BTCUSDT,ETHUSDT",
    "EXCLUDE_SYMBOLS": "",

    # Spot↔Perp basis
    "SPOT_PERP_ENABLED": "true",
    "SPOT_PERP_Z": "2.7",
    "SPOT_PERP_SYNC_TOL_SEC": "30",
    "SPOT_PERP_EXCHANGES": "binance,bybit,okx",

    # Supabase
    "SUPABASE_ENABLED": "false",
    "SUPABASE_URL": "",
    "SUPABASE_KEY": "",
}

_VALID_INTERVALS = {"1m", "3m", "5m", "15m", "1h"}

# ---------- helpers ----------
def _raw_env(k: str) -> str | None:
    v = os.getenv(k)
    return v if v not in (None, "") else None

def _sanitize(v: str) -> str:
    s = v.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s.replace("\\", "").strip()

def _get(k: str) -> str:
    v = _raw_env(k)
    return _sanitize(v) if v is not None else DEFAULTS[k]

def _split_csv(v: str) -> list[str]:
    return [p.strip() for p in _sanitize(v).split(",") if p.strip()]

def _to_bool(v: str) -> bool:
    return _sanitize(v).lower() in {"1","true","on","yes","y"}

def _to_int(v: str, d: int) -> int:
    try: return int(_sanitize(v))
    except Exception: return int(d)

def _to_float(v: str, d: float) -> float:
    try: return float(_sanitize(v))
    except Exception: return float(d)

# ---------- settings ----------
class Settings(BaseModel):
    # Core
    exchanges: list[str] = Field(default_factory=lambda: _split_csv(DEFAULTS["EXCHANGES"]))
    symbols: list[str]   = Field(default_factory=lambda: _split_csv(DEFAULTS["SYMBOLS"]))
    interval: str = DEFAULTS["INTERVAL"]
    lookback: int = int(DEFAULTS["LOOKBACK_CANDLES"])

    # Alerts / risk
    discord_webhook: str | None = None
    risk_off: bool = False
    alert_cooldown_sec: int = int(DEFAULTS["ALERT_COOLDOWN_SEC"])
    scan_period_sec: int = int(DEFAULTS["SCAN_PERIOD_SEC"])
    alert_min_score: float = float(DEFAULTS["ALERT_MIN_SCORE"])
    momentum_z: float = float(DEFAULTS["MOMENTUM_Z"])
    max_pos_usdt: float = float(DEFAULTS["MAX_POSITION_PER_SYMBOL_USDT"])

    # Hotlist
    hotlist_enabled: bool = True
    hotlist_top_n: int = 20
    hotlist_min_vol_usdt: float = 200_000.0
    force_symbols: list[str] = Field(default_factory=lambda: _split_csv(DEFAULTS["FORCE_SYMBOLS"]))
    exclude_symbols: list[str] = Field(default_factory=list)

    # Spot↔Perp
    spot_perp_enabled: bool = True
    spot_perp_z: float = 2.7
    spot_perp_sync_tol_sec: int = 30
    spot_perp_exchanges: list[str] = Field(default_factory=lambda: _split_csv(DEFAULTS["SPOT_PERP_EXCHANGES"]))

    # Supabase
    supabase_enabled: bool = False
    supabase_url: str | None = None
    supabase_key: str | None = None

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

        hotlist_enabled=_to_bool(_get("HOTLIST_ENABLED")),
        hotlist_top_n=_to_int(_get("HOTLIST_TOP_N"), int(DEFAULTS["HOTLIST_TOP_N"])),
        hotlist_min_vol_usdt=_to_float(_get("HOTLIST_MIN_VOL_USDT"), float(DEFAULTS["HOTLIST_MIN_VOL_USDT"])),
        force_symbols=_split_csv(_get("FORCE_SYMBOLS")),
        exclude_symbols=_split_csv(_get("EXCLUDE_SYMBOLS")),

        spot_perp_enabled=_to_bool(_get("SPOT_PERP_ENABLED")),
        spot_perp_z=_to_float(_get("SPOT_PERP_Z"), float(DEFAULTS["SPOT_PERP_Z"])),
        spot_perp_sync_tol_sec=_to_int(_get("SPOT_PERP_SYNC_TOL_SEC"), int(DEFAULTS["SPOT_PERP_SYNC_TOL_SEC"])),
        spot_perp_exchanges=_split_csv(_get("SPOT_PERP_EXCHANGES")),

        supabase_enabled=_to_bool(_get("SUPABASE_ENABLED")),
        supabase_url=_get("SUPABASE_URL") or None,
        supabase_key=_get("SUPABASE_KEY") or None,
    )
    s.validate_interval()
    return s
