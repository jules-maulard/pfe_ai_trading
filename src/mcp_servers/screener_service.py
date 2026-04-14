from __future__ import annotations

import pandas as pd
from typing import Dict, List, Optional

from ..data import BaseStorage
from ..data.config.settings import get_storage
from ..utils import get_logger

logger = get_logger(__name__)


class ScreenerService:
    def __init__(self, storage: BaseStorage = None):
        self.storage = storage or get_storage()

    def get_volume_anomalies(
        self,
        limit: int = 3,
        window: int = 20,
        multiplier: float = 2.0,
    ) -> List[str]:
        df = self.storage.load_ohlcv()
        df = df.sort_values(["symbol", "date"])

        anomalies: List[str] = []
        for symbol, group in df.groupby("symbol"):
            if len(group) < window:
                continue
            recent = group.tail(window)
            avg_volume = recent["volume"].mean()
            last_volume = group["volume"].iloc[-1]
            if last_volume > avg_volume * multiplier:
                anomalies.append(str(symbol))

        return sorted(anomalies)[:limit]

    def get_top_movers(
        self,
        limit: int = 3,
    ) -> Dict[str, List[str]]:
        df = self.storage.load_ohlcv()
        df = df.sort_values(["symbol", "date"])

        changes: Dict[str, float] = {}
        for symbol, group in df.groupby("symbol"):
            if len(group) < 2:
                continue
            prev_close = group["close"].iloc[-2]
            last_close = group["close"].iloc[-1]
            if prev_close == 0:
                continue
            changes[str(symbol)] = ((last_close - prev_close) / prev_close) * 100

        sorted_symbols = sorted(changes.items(), key=lambda x: x[1], reverse=True)

        top_gainers = [s for s, _ in sorted_symbols[:limit]]
        top_losers = [s for s, _ in sorted_symbols[-limit:]]

        return {
            "top_gainers": top_gainers,
            "top_losers": top_losers,
        }
