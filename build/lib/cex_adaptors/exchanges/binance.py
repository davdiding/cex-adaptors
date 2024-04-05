from .base import BaseClient


class BinanceSpot(BaseClient):
    BASE_ENDPOINT = "https://api{}.binance.com"
    name = "binance"

    def __init__(self, api_key: str, api_secret: str, api_version: int = 3):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT.format(api_version)

        self.auth_data = {
            "api_key": api_key,
            "api_secret": api_secret,
        }

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

    # Private endpoint
    async def _get_account_info(self):
        return await self._get(self.base_endpoint + "/api/v3/account", auth_data=self.auth_data)

    async def _get_margin_account_info(self):
        return await self._get(self.base_endpoint + "/sapi/v1/margin/account", auth_data=self.auth_data)


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

    async def _get_funding_rate_history(
        self, symbol: str, startTime: int = None, endTime: int = None, limit: int = 1000
    ):
        params = {
            k: v
            for k, v in {
                "symbol": symbol,
                "startTime": startTime,
                "endTime": endTime,
                "limit": limit,
            }.items()
            if v
        }
        return await self._get(self.linear_base_endpoint + "/fapi/v1/fundingRate", params=params)


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

    async def _get_funding_rate_history(
        self, symbol: str, startTime: int = None, endTime: int = None, limit: int = 1000
    ):
        params = {
            k: v
            for k, v in {
                "symbol": symbol,
                "startTime": startTime,
                "endTime": endTime,
                "limit": limit,
            }.items()
            if v
        }
        return await self._get(self.inverse_base_endpoint + "/dapi/v1/fundingRate", params=params)
