from abc import ABC, abstractmethod
from typing import List

class ExchangePublic(ABC):
    @abstractmethod
    async def fetch_klines(self, symbol: str, interval: str, limit: int) -> list[list]:
        """Return list of [ts_ms, open, high, low, close, volume]"""
        ...
