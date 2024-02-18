from .base import BaseClient


class HtxUnified(BaseClient):
    BASE_URL = "https://api.huobi.pro"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_URL

    async def _get_exchange_info(self):
        return await self._get(self.base_endpoint + "/v2/settings/common/symbols")

    async def _get_tickers(self):
        return await self._get(self.base_endpoint + "/market/detail")


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
