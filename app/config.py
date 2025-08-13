from pydantic import BaseModel
import os

class Settings(BaseModel):
    exchanges: list[str] = []
    symbols: list[str] = []
    interval: str = "1m"
    lookback: int = 500
    discord_webhook: str | None = None
    max_pos_usdt: float = 200.0
    risk_off: bool = False

    kucoin_key: str | None = None
    kucoin_secret: str | None = None
    kucoin_passphrase: str | None = None

    mexc_key: str | None = None
    mexc_secret: str | None = None

def load_settings() -> Settings:
    return Settings(
        exchanges=[x.strip() for x in os.getenv("EXCHANGES", "kucoin").split(",") if x.strip()],
        symbols=[x.strip() for x in os.getenv("SYMBOLS", "BTCUSDT").split(",") if x.strip()],
        interval=os.getenv("INTERVAL","1m"),
        lookback=int(os.getenv("LOOKBACK_CANDLES","500")),
        discord_webhook=os.getenv("DISCORD_WEBHOOK"),
        max_pos_usdt=float(os.getenv("MAX_POSITION_PER_SYMBOL_USDT","200")),
        risk_off=os.getenv("RISK_OFF","false").lower()=="true",
        kucoin_key=os.getenv("KUCOIN_API_KEY"),
        kucoin_secret=os.getenv("KUCOIN_API_SECRET"),
        kucoin_passphrase=os.getenv("KUCOIN_API_PASSPHRASE"),
        mexc_key=os.getenv("MEXC_API_KEY"),
        mexc_secret=os.getenv("MEXC_API_SECRET"),
    )
