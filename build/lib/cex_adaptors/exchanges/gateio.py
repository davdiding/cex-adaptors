from .base import BaseClient


class GateioClient(BaseClient):
    BASE_URL = "https://api.gateio.ws/api/v4"

    def __init__(self) -> None:
        super().__init__()
        self.base_url = self.BASE_URL

    async def _get_currency_pairs(self):
        return await self._get(self.base_url + "/spot/currency_pairs")

    async def _get_perp_info(self, settle: str):
        return await self._get(self.base_url + f"/futures/{settle}/contracts")

    async def _get_futures_info(self, settle: str = "usdt"):
        return await self._get(self.base_url + f"/delivery/{settle}/contracts")

    async def _get_spot_tickers(self, currency_pair: str = None, timezone: str = None):
        params = {}
        if currency_pair:
            params["currency_pair"] = currency_pair
        if timezone:
            params["timezone"] = timezone
        return await self._get(self.base_url + "/spot/tickers", params=params)

    async def _get_perp_tickers(self, settle: str):
        return await self._get(self.base_url + f"/futures/{settle}/tickers")

    async def _get_futures_tickers(self, settle: str = "usdt"):
        return await self._get(self.base_url + f"/delivery/{settle}/tickers")

    async def _get_spot_klines(self, symbol: str, interval: str, start: int = None, end: int = None, limit: int = None):
        params = {"currency_pair": symbol, "interval": interval}
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        if limit:
            params["limit"] = limit

        return await self._get(self.base_url + "/spot/candlesticks", params=params)

    async def _get_perp_klines(
        self, symbol: str, settle: str, interval: str, start: int = None, end: int = None, limit: int = None
    ):
        params = {"contract": symbol, "interval": interval}
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        if limit:
            params["limit"] = limit

        return await self._get(self.base_url + f"/futures/{settle}/candlesticks", params=params)

    async def _get_futures_klines(
        self, symbol: str, settle: str, interval: str, start: int = None, end: int = None, limit: int = None
    ):
        params = {"contract": symbol, "interval": interval}
        if start:
            params["from"] = start
        if end:
            params["to"] = end

        if not start and not end and limit:
            params["limit"] = limit

        return await self._get(self.base_url + f"/delivery/{settle}/candlesticks", params=params)
