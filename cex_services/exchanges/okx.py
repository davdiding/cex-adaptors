import base64
import hmac
from datetime import datetime as dt

from .base import BaseClient


class OkxUnified(BaseClient):
    BASE_ENDPOINT = "https://www.okx.com"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        use_server_time: bool = False,
        debug: bool = False,
        flag: str = "1",
    ):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.use_server_time = use_server_time
        self.debug = debug
        self.flag = flag

    def get_private_header(self, method, request_path, body):
        timestamp = self.get_timestamp()
        message = str(timestamp) + str.upper(method) + request_path + body
        sign = self.get_signature(message)

        header = dict()
        header["Content-Type"] = "application/json"
        header["OK-ACCESS-KEY"] = self.api_key
        header["OK-ACCESS-SIGN"] = sign
        header["OK-ACCESS-TIMESTAMP"] = str(timestamp)
        header["OK-ACCESS-PASSPHRASE"] = self.passphrase
        header["x-simulated-trading"] = self.flag

        return header

    def get_signature(self, message: str):
        mac = hmac.new(bytes(self.api_secret, encoding="utf8"), bytes(message, encoding="utf-8"), digestmod="sha256")
        d = mac.digest()
        return base64.b64encode(d).decode("utf-8")

    @staticmethod
    def get_timestamp():
        now = dt.utcnow()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

    async def _get_exchange_info(self, instType: str) -> dict:
        return await self._get(self.BASE_ENDPOINT + "/api/v5/public/instruments", params={"instType": instType})

    async def _get_tickers(self, instType: str):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/tickers", params={"instType": instType})

    async def _get_ticker(self, instId: str):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/ticker", params={"instId": instId})

    async def _get_klines(self, instId: str, bar: str, after: int = None, before: int = None, limit: int = None):
        params = {"instId": instId, "bar": bar}
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        if limit:
            params["limit"] = limit
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/candles", params=params)

    async def _get_balance(self, currency: str = None):
        params = {}
        if currency:
            params["ccy"] = currency
        request_path = "/api/v5/account/balance"

        return await self._get(
            self.BASE_ENDPOINT + request_path, params=params, headers=self.get_private_header("get", request_path, "")
        )

    async def _get_positions(self):
        request_path = "/api/v5/account/positions"
        return await self._get(
            self.BASE_ENDPOINT + "/api/v5/account/positions", headers=self.get_private_header("get", request_path, "")
        )
