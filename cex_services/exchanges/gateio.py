from .base import BaseClient


class GateioClient(BaseClient):
    BASE_URL = "https://api.gateio.ws/api/v4"

    def __init__(self) -> None:
        super().__init__()
        self.base_url = self.BASE_URL

    async def _get_currency_pairs(self):
        return await self._get(self.base_url + "/spot/currency_pairs")
