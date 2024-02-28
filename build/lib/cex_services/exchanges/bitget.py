from .base import BaseClient


class BitgetUnified(BaseClient):
    BASE_URL = "https://api.bitget.com"

    def __init__(self) -> None:
        super().__init__()
        self.base_endpoint = self.BASE_URL

    async def _get_spot_exchange_info(self):
        return await self._get(self.base_endpoint + "/api/v2/spot/public/symbols")

    async def _get_derivative_exchange_info(self, product_type: str):
        params = {"productType": product_type}
        return await self._get(self.base_endpoint + "/api/v2/mix/market/contracts", params=params)

    async def _get_spot_tickers(self):
        return await self._get(self.base_endpoint + "/api/v2/spot/market/tickers")

    async def _get_derivative_tickers(self, product_type: str):
        params = {"productType": product_type}
        return await self._get(self.base_endpoint + "/api/v2/mix/market/tickers", params=params)

    async def _get_spot_candlesticks(
        self, symbol: str, granularity: str, startTime: str = None, endTime: str = None, limit: str = None
    ):
        params = {"symbol": symbol, "granularity": granularity}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        if limit:
            params["limit"] = limit
        return await self._get(self.base_endpoint + "/api/v2/spot/market/candles", params=params)

    async def _get_derivative_candlesticks(
        self,
        symbol: str,
        productType: str,
        granularity: str,
        startTime: str = None,
        endTime: str = None,
        kLineType: str = "MARKET",
        limit: str = None,
    ):
        params = {"symbol": symbol, "productType": productType, "granularity": granularity, "kLineType": kLineType}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        if limit:
            params["limit"] = limit
        return await self._get(self.base_endpoint + "/api/v2/mix/market/candles", params=params)
