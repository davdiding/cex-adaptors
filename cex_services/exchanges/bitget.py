from .base import BaseClient


class BitgetUnified(BaseClient):
    BASE_URL = "https://api.bitget.com"

    def __init__(self) -> None:
        super().__init__()
        self.base_endpoint = self.BASE_URL

    def _get_spot_exchange_info(self):
        return self._get(self.base_endpoint + "/api/v2/spot/public/symbols")

    def _get_derivative_exchange_info(self, product_type: str):
        params = {"productType": product_type}
        return self._get(self.base_endpoint + "/api/v2/mix/market/contracts", params=params)

    def _get_spot_tickers(self):
        return self._get(self.base_endpoint + "/api/v2/spot/market/tickers")

    def _get_derivative_tickers(self, product_type: str):
        params = {"productType": product_type}
        return self._get(self.base_endpoint + "/api/v2/mix/market/tickers", params=params)
