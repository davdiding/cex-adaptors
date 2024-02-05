from .base import BaseClient


class OkxUnified(BaseClient):
    BASE_ENDPOINT = "https://www.okx.com"

    def __init__(self):
        super().__init__()

    async def _get_exchange_info(self, instType: str) -> dict:
        return await self._get(self.BASE_ENDPOINT + "/api/v5/public/instruments", params={"instType": instType})

    async def _get_tickers(self, instType: str):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/tickers", params={"instType": instType})

    async def _get_ticker(self, instId: str):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/ticker", params={"instId": instId})

    async def _get_klines(self, instId: str, bar: str, after: int = None, before: int = None, limit: int = None):
        params = {"instId": instId, "bar": bar}
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        if limit:
            params["limit"] = limit
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/candles", params=params)
