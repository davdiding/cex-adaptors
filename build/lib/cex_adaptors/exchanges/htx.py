from .base import BaseClient


class HtxSpot(BaseClient):
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

    async def _get_linear_funding_fee(self, contract_code: str):
        params = {"contract_code": contract_code}
        return await self._get(self.base_endpoint + "/linear-swap-api/v1/swap_funding_rate", params=params)

    async def _get_inverse_perp_funding_fee(self, contract_code: str):
        params = {"contract_code": contract_code}
        return await self._get(self.base_endpoint + "/swap-api/v1/swap_funding_rate", params=params)

    async def _get_linear_history_funding_rate(self, contract_code: str, page_index: int = None, page_size: int = None):
        params = {
            k: v
            for k, v in {"contract_code": contract_code, "page_index": page_index, "page_size": page_size}.items()
            if v
        }
        return await self._get(self.base_endpoint + "/linear-swap-api/v1/swap_historical_funding_rate", params=params)

    async def _get_inverse_perp_history_funding_rate(
        self, contract_code: str, page_index: int = None, page_size: int = None
    ):
        params = {
            k: v
            for k, v in {"contract_code": contract_code, "page_index": page_index, "page_size": page_size}.items()
            if v
        }
        return await self._get(self.base_endpoint + "/swap-api/v1/swap_historical_funding_rate", params=params)

    async def _get_linear_index(self, contract_code: str):
        params = {"contract_code": contract_code}
        return await self._get(self.base_endpoint + "/linear-swap-api/v1/swap_index", params=params)

    async def _get_inverse_futures_index(self, symbol: str):
        params = {"symbol": symbol}
        return await self._get(self.base_endpoint + "/api/v1/contract_index", params=params)

    async def _get_inverse_swap_index(self, contract_code: str):
        params = {"contract_code": contract_code}
        return await self._get(self.base_endpoint + "/swap-api/v1/swap_index", params=params)

    async def _get_linear_market_data(self, contract_code: str):
        params = {"contract_code": contract_code}
        return await self._get(self.base_endpoint + "/linear-swap-ex/market/detail/merged", params=params)

    async def _get_linear_kline_data_of_mark_price(self, contract_code: str, period: str, size: int):
        params = {"contract_code": contract_code, "period": period, "size": size}
        return await self._get(self.base_endpoint + "/index/market/history/linear_swap_mark_price_kline", params=params)

    async def _get_inverse_futures_kline_data_of_mark_price(self, symbol: str, period: str, size: int):
        params = {"symbol": symbol, "period": period, "size": size}
        return await self._get(self.base_endpoint + "/index/market/history/mark_price_kline", params=params)

    async def _get_inverse_swap_kline_data_of_mark_price(self, contract_code: str, period: str, size: int):
        params = {"contract_code": contract_code, "period": period, "size": size}
        return await self._get(self.base_endpoint + "/index/market/history/swap_mark_price_kline", params=params)
