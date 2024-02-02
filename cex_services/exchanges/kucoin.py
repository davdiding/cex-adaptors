from .base import BaseClient


class KucoinSpot(BaseClient):
    BASE_ENDPOINT = "https://api.kucoin.com"

    def __init__(self) -> None:
        super().__init__()
        self.spot_base_endpoint = self.BASE_ENDPOINT

    async def _get_currency_list(self):
        return await self._get(self.spot_base_endpoint + "/api/v3/currencies")

    async def _get_symbol_list(self):
        return await self._get(self.spot_base_endpoint + "/api/v2/symbols")

    async def _get_tickers(self):
        return await self._get(self.spot_base_endpoint + "/api/v1/market/allTickers")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.spot_base_endpoint + f" /api/v1/market/orderbook/level1?symbol={symbol}")

    async def _get_24hr_stats(self, symbol: str):
        return await self._get(self.spot_base_endpoint + f"/api/v1/market/stats?symbol={symbol}")

    async def _get_klines(self, symbol: str, type: str, startAt: int = None, endAt: int = None):
        params = {
            "symbol": symbol,
            "type": type,
        }
        if startAt:
            params["startAt"] = startAt

        if endAt:
            params["endAt"] = endAt

        return await self._get(self.spot_base_endpoint + "/api/v1/market/candles", params=params)


class KucoinFutures(BaseClient):
    BASE_ENDPOINT = "https://api-futures.kucoin.com"

    def __init__(self) -> None:
        super().__init__()
        self.futures_base_endpoint = self.BASE_ENDPOINT

    async def _get_symbol_list(self):
        return await self._get(self.futures_base_endpoint + "/api/v1/contracts/active")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.futures_base_endpoint + f"/api/v1/ticker?symbol={symbol}")
