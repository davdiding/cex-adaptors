from .base import BaseClient


class BybitUnified(BaseClient):
    name = "bybit"
    BASE_ENDPOINT = "https://api.bybit.com"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT

    async def _get_exchange_info(self, category: str) -> dict:
        return await self._get(self.base_endpoint + "/v5/market/instruments-info", params={"category": category})

    async def _get_tickers(self, category: str) -> dict:
        return await self._get(self.base_endpoint + "/v5/market/tickers", params={"category": category})

    async def _get_ticker(self, symbol: str, category: str):
        params = {"symbol": symbol, "category": category}
        return await self._get(self.base_endpoint + "/v5/market/tickers", params=params)

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

    async def _get_funding_rate(
        self, category: str, symbol: str, startTime: int = None, endTime: int = None, limit: int = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "start_time": startTime,
                "end_time": endTime,
                "limit": limit,
            }.items()
            if v is not None
        }

        return await self._get(self.base_endpoint + "/v5/market/funding/history", params=params)

    async def _get_open_interest(
        self, category: str, symbol: str, interval: str, startTime: int = None, endTime: int = None, limit: int = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "intervalTime": interval,
                "start_time": startTime,
                "end_time": endTime,
                "limit": limit,
            }.items()
            if v
        }

        return await self._get(self.base_endpoint + "/v5/market/open-interest", params=params)

    async def _get_orderbook(self, category: str, symbol: str, limit: int = None):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "limit": limit,
            }.items()
            if v
        }

        return await self._get(self.base_endpoint + "/v5/market/orderbook", params=params)
