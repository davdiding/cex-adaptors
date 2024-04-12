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

    async def _get_24hr_ticker(self, symbol: str):
        params = {"symbol": symbol}
        return await self._get(self.spot_base_endpoint + "/api/v1/market/stats", params=params)

    async def _get_24hr_stats(self, symbol: str):
        return await self._get(self.spot_base_endpoint + f"/api/v1/market/stats?symbol={symbol}")

    async def _get_klines(self, symbol: str, type: str, start: int = None, end: int = None):
        params = {
            "symbol": symbol,
            "type": type,
        }
        if start:
            params["startAt"] = start

        if end:
            params["endAt"] = end

        return await self._get(self.spot_base_endpoint + "/api/v1/market/candles", params=params)

    async def _get_margin_mark_price(self, symbol: str):
        return await self._get(self.spot_base_endpoint + f"/api/v1/mark-price/{symbol}/current")

    async def _get_part_orderbook(self, symbol: str, depth: int):
        if depth not in [20, 100]:
            raise ValueError("Depth must be 20 or 100 in KucoinSpot")
        return await self._get(
            self.spot_base_endpoint + f"/api/v1/market/orderbook/level2_{depth}", params={"symbol": symbol}
        )

    async def _get_full_orderbook(self, symbol: str):
        return await self._get(self.spot_base_endpoint + f"/api/v3/market/orderbook/level2", params={"symbol": symbol})


class KucoinFutures(BaseClient):
    BASE_ENDPOINT = "https://api-futures.kucoin.com"

    def __init__(self) -> None:
        super().__init__()
        self.futures_base_endpoint = self.BASE_ENDPOINT

    async def _get_symbol_list(self):
        return await self._get(self.futures_base_endpoint + "/api/v1/contracts/active")

    async def _get_ticker(self, symbol: str):
        params = {"symbol": symbol}
        return await self._get(self.futures_base_endpoint + f"/api/v1/ticker", params=params)

    async def _get_symbol_detail(self, symbol: str):
        return await self._get(self.futures_base_endpoint + f"/api/v1/contracts/{symbol}")

    async def _get_klines(self, symbol: str, granularity: int, start: int = None, end: int = None):

        params = {
            "symbol": symbol,
            "granularity": granularity,
        }
        if start:
            params["from"] = start
        if end:
            params["to"] = end

        return await self._get(self.futures_base_endpoint + "/api/v1/kline/query", params=params)

    async def _get_current_mark_price(self, symbol: str):
        return await self._get(self.futures_base_endpoint + f"/api/v1/mark-price/{symbol}/current")

    async def _get_part_orderbook(self, symbol: str, depth: int):
        if depth not in [20, 100]:
            raise ValueError("Depth must be 20 or 100 in KucoinSpot")
        return await self._get(self.futures_base_endpoint + f"/api/v1/level2/depth{depth}", params={"symbol": symbol})

    async def _get_full_orderbook(self, symbol: str):
        return await self._get(self.futures_base_endpoint + "/api/v1/level2/snapshot", params={"symbol": symbol})

    async def _get_current_funding_rate(self, symbol: str):
        return await self._get(self.futures_base_endpoint + f"/api/v1/funding-rate/{symbol}/current")

    async def _get_public_funding_history(self, symbol: str, _from: int, to: int):
        params = {
            "symbol": symbol,
            "from": _from,
            "to": to,
        }
        return await self._get(self.futures_base_endpoint + "/api/v1/contract/funding-rates", params=params)

    async def _get_private_funding_history(
        self,
        symbol: str,
        startAt: int = None,
        endAt: int = None,
        reverse: bool = None,
        offset: int = None,
        forward: bool = None,
        maxCount: int = None,
    ):
        params = {
            k: v
            for k, v in {
                "symbol": symbol,
                "startAt": startAt,
                "endAt": endAt,
                "reverse": reverse,
                "offset": offset,
                "forward": forward,
                "maxCount": maxCount,
            }.items()
            if v is not None
        }
        return await self._get(self.futures_base_endpoint + "/api/v1/funding-history", params=params)
