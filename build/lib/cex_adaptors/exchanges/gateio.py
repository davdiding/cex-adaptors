from .base import BaseClient


class GateioUnified(BaseClient):
    BASE_URL = "https://api.gateio.ws/api/v4"

    def __init__(self) -> None:
        super().__init__()
        self.base_url = self.BASE_URL

    async def _get_currency_pairs(self):
        return await self._get(self.base_url + "/spot/currency_pairs")

    async def _get_perp_info(self, settle: str, contract: str = None):
        params = {k: v for k, v in {"contract": contract}.items() if v}
        return await self._get(self.base_url + f"/futures/{settle}/contracts", params=params)

    async def _get_futures_info(self, settle: str = "usdt"):
        return await self._get(self.base_url + f"/delivery/{settle}/contracts")

    async def _get_spot_tickers(self, currency_pair: str = None, timezone: str = None):
        params = {k: v for k, v in {"currency_pair": currency_pair, "timezone": timezone}.items() if v}
        return await self._get(self.base_url + "/spot/tickers", params=params)

    async def _get_perp_tickers(self, settle: str, contract: str = None):
        params = {k: v for k, v in {"contract": contract}.items() if v}
        return await self._get(self.base_url + f"/futures/{settle}/tickers", params=params)

    async def _get_futures_tickers(self, settle: str = "usdt", contract: str = None):
        params = {k: v for k, v in {"contract": contract}.items() if v}
        return await self._get(self.base_url + f"/delivery/{settle}/tickers", params=params)

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

    async def _get_futures_funding_rate_history(self, settle: str, contract: str, limit: int = None):
        params = {k: v for k, v in {"contract": contract, "limit": limit}.items() if v}
        return await self._get(self.base_url + f"/futures/{settle}/funding_rate", params=params)

    async def _get_perp_premium_index_kline(
        self, settle: str, contract: str, _from: int = None, to: int = None, limit: int = None, interval: str = None
    ):
        params = {
            k: v
            for k, v in {"contract": contract, "from": _from, "to": to, "limit": limit, "interval": interval}.items()
            if v
        }
        return await self._get(self.base_url + f"/futures/{settle}/premium_index", params=params)

    async def _get_perp_premium_index_klines(
        self, settle: str, contract: str, interval: str, _from: int = None, to: int = None, limit: int = None
    ):
        params = {
            k: v
            for k, v in {"contract": contract, "from": _from, "to": to, "limit": limit, "interval": interval}.items()
            if v
        }
        return await self._get(self.base_url + f"/futures/{settle}/premium_index", params=params)
