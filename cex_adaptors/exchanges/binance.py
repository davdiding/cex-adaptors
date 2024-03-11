from .base import BaseClient


class BinanceSpot(BaseClient):
    BASE_ENDPOINT = "https://api{}.binance.com"

    def __init__(self, api_version: int = 3):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT.format(api_version)

    async def _get_exchange_info(self):
        return await self._get(self.base_endpoint + "/api/v3/exchangeInfo")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.base_endpoint + "/api/v3/ticker/24hr", params={"symbol": symbol})

    async def _get_tickers(self):
        return await self._get(self.base_endpoint + "/api/v3/ticker/24hr")

    async def _get_klines(
        self,
        symbol: str,
        interval: str,
        startTime: int = None,
        endTime: int = None,
        limit: int = 500,
        timeZone: str = "0",
    ):
        params = {"symbol": symbol, "interval": interval, "limit": limit, "timeZone": timeZone}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        return await self._get(self.base_endpoint + "/api/v3/klines", params=params)


class BinanceLinear(BaseClient):
    BASE_ENDPOINT = "https://fapi.binance.com"

    def __init__(self) -> None:
        super().__init__()
        self.linear_base_endpoint = self.BASE_ENDPOINT

    async def _get_exchange_info(self):
        return await self._get(self.linear_base_endpoint + "/fapi/v1/exchangeInfo")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.linear_base_endpoint + "/fapi/v1/ticker/24hr", params={"symbol": symbol})

    async def _get_tickers(self):
        return await self._get(self.linear_base_endpoint + "/fapi/v1/ticker/24hr")

    async def _get_klines(
        self, symbol: str, interval: str, startTime: int = None, endTime: int = None, limit: int = 500
    ):
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        return await self._get(self.linear_base_endpoint + "/fapi/v1/klines", params=params)


class BinanceInverse(BaseClient):
    BASE_ENDPOINT = "https://dapi.binance.com"

    def __init__(self) -> None:
        super().__init__()
        self.inverse_base_endpoint = self.BASE_ENDPOINT

    async def _get_exchange_info(self):
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/exchangeInfo")

    async def _get_ticker(self, symbol: str):
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/ticker/24hr", params={"symbol": symbol})

    async def _get_tickers(self):
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/ticker/24hr")

    async def _get_klines(
        self, symbol: str, interval: str, startTime: int = None, endTime: int = None, limit: int = 500
    ):
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/klines", params=params)
