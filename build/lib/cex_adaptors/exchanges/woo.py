from .base import BaseClient


class WOOUnified(BaseClient):
    name = "woo"
    BASE_ENDPOINT = "https://api.woo.org"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT

    async def _get_available_symbols(self):
        return await self._get(self.base_endpoint + "/v1/public/info")
