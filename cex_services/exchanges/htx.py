from .base import BaseClient


class HtxUnified(BaseClient):
    BASE_URL = "https://api.huobi.pro"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_URL

    def _get_exchange_info(self):
        return self._get(self.base_endpoint + "/v2/settings/common/symbols")


class HtxFutures(BaseClient):
    BASE_URL = "https://api.hbdm.com"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_URL
