from .base import BaseClient


class BybitUnified(BaseClient):
    BASE_ENDPOINT = "https://api.bybit.com"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT

    async def _get_exchange_info(self, category: str) -> dict:
        return await self._get(self.base_endpoint + "/v5/market/instruments-info", params={"category": category})

    async def _get_tickers(self, category: str) -> dict:
        return await self._get(self.base_endpoint + "/v5/market/tickers", params={"category": category})

    async def _get_klines(
        self, symbol: str, interval: str, category: str = None, start: int = None, end: int = None, limit: int = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "interval": interval,
                "start": start,
                "end": end,
                "limit": limit,
            }.items()
            if v is not None
        }

        return await self._get(self.base_endpoint + "/v5/market/kline", params=params)
