from .base import BaseClient


class HtxUnified(BaseClient):
    BASE_URL = "https://api.huobi.pro"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_URL

    async def _get_exchange_info(self):
        return await self._get(self.base_endpoint + "/v2/settings/common/symbols")

    async def _get_tickers(self):
        return await self._get(self.base_endpoint + "/market/tickers")

    async def _get_klines(self, symbol: str, period: str, limit: int):
        params = {"symbol": symbol, "period": period, "size": limit}

        return await self._get(self.base_endpoint + "/market/history/kline", params=params)


class HtxFutures(BaseClient):
    BASE_URL = "https://api.hbdm.com"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_URL

    async def _get_linear_contract_info(self):
        return await self._get(self.base_endpoint + "/linear-swap-api/v1/swap_contract_info")

    async def _get_inverse_futures_info(self):
        return await self._get(self.base_endpoint + "/api/v1/contract_contract_info")

    async def _get_inverse_perp_info(self):
        return await self._get(self.base_endpoint + "/swap-api/v1/swap_contract_info")

    async def _get_linear_contract_tickers(self):
        return await self._get(self.base_endpoint + "/v2/linear-swap-ex/market/detail/batch_merged")

    async def _get_inverse_perp_tickers(self):
        return await self._get(self.base_endpoint + "/v2/swap-ex/market/detail/batch_merged")

    async def _get_inverse_futures_tickers(self):
        return await self._get(self.base_endpoint + "/v2/market/detail/batch_merged")

    async def _get_linear_contract_klines(
        self, symbol: str, period: str, start: int = None, end: int = None, limit: int = None
    ):
        params = {"contract_code": symbol, "period": period}
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        if limit:
            params["size"] = limit

        return await self._get(self.base_endpoint + "/linear-swap-ex/market/history/kline", params=params)

    async def _get_inverse_perp_klines(
        self, symbol: str, period: str, start: int = None, end: int = None, limit: int = None
    ):
        params = {"contract_code": symbol, "period": period}
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        if limit:
            params["size"] = limit

        return await self._get(self.base_endpoint + "/swap-ex/market/history/kline", params=params)

    async def _get_inverse_futures_klines(
        self, symbol: str, period: str, start: int = None, end: int = None, limit: int = None
    ):
        params = {"symbol": symbol, "period": period}
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        if limit:
            params["size"] = limit

        return await self._get(self.base_endpoint + "/market/history/kline", params=params)
