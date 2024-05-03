from .base import BaseClient


class OkxUnified(BaseClient):
    name = "okx"
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

        self.auth_data = (
            {
                "api_key": self.api_key,
                "api_secret": self.api_secret,
                "passphrase": self.passphrase,
                "use_server_time": self.use_server_time,
                "debug": self.debug,
                "flag": self.flag,
            }
            if self.api_key
            else {}
        )

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

    async def _get_current_funding_rate(self, instId: str):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/public/funding-rate", params={"instId": instId})

    async def _get_history_funding_rate(self, instId: str, after: int = None, before: int = None, limit: int = None):
        params = {k: v for k, v in {"instId": instId, "after": after, "before": before, "limit": limit}.items() if v}
        return await self._get(self.BASE_ENDPOINT + "/api/v5/public/funding-rate-history", params=params)

    async def _get_index_ticker(self, instId: str, quoteCcy: str):
        params = {k: v for k, v in {"instId": instId, "quoteCcy": quoteCcy}.items() if v}
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/index-tickers", params=params)

    async def _get_mark_price(self, instId: str, instType: str):
        return await self._get(
            self.BASE_ENDPOINT + "/api/v5/public/mark-price", params={"instId": instId, "instType": instType}
        )

    async def _get_open_interest(self, instId: str = None, instType: str = None):
        params = {k: v for k, v in {"instId": instId, "instType": instType}.items() if v}
        return await self._get(self.BASE_ENDPOINT + "/api/v5/public/open-interest", params={**params})

    async def _get_orderbook(self, instId: str, sz: str):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/market/books", params={"instId": instId, "sz": sz})

    # Private endpoints

    async def _get_balance(self, currency: str = None):
        params = {}
        if currency:
            params["ccy"] = currency

        return await self._get(self.BASE_ENDPOINT + "/api/v5/account/balance", params=params)

    async def _get_positions(self):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/account/positions")

    async def _get_account_config(self):
        return await self._get(self.BASE_ENDPOINT + "/api/v5/account/config")

    async def _get_order_info(self, instId: str, ordId: str):
        return await self._get(
            self.BASE_ENDPOINT + "/api/v5/trade/order",
            params={"instId": instId, "ordId": ordId},
        )

    async def _place_order(
        self,
        instId: str,
        side: str,
        sz: str,
        px: str = None,
        tgtCcy: str = "base_ccy",
        ordType: str = "market",
        tdMode: str = "cross",
        **kwargs
    ):
        params = {
            k: v
            for k, v in {
                "instId": instId,
                "tdMode": tdMode,
                "side": side,
                "ordType": ordType,
                "sz": sz,
                "px": px,
                "tgtCcy": tgtCcy,
            }.items()
            if v
        }
        return await self._post(self.BASE_ENDPOINT + "/api/v5/trade/order", auth_data=self.auth_data, params=params)

    async def _cancel_order(self, instId: str, ordId: str):
        return await self._post(
            self.BASE_ENDPOINT + "/api/v5/trade/cancel-order",
            params={"instId": instId, "ordId": ordId},
        )

    async def _get_opened_orders(
        self,
        instType: str = None,
        uly: str = None,
        instFamily: str = None,
        instId: str = None,
        ordType: str = None,
        state: str = None,
        after: str = None,
        before: str = None,
        limit: str = None,
    ):
        params = {
            k: v
            for k, v in {
                "instType": instType,
                "uly": uly,
                "instFamily": instFamily,
                "instId": instId,
                "ordType": ordType,
                "state": state,
                "after": after,
                "before": before,
                "limit": limit,
            }.items()
            if v
        }

        return await self._get(
            self.BASE_ENDPOINT + "/api/v5/trade/orders-pending", auth_data=self.auth_data, params=params
        )

    async def _get_history_orders(
        self,
        instType: str,
        uly: str = None,
        instFamily: str = None,
        instId: str = None,
        ordType: str = None,
        state: str = None,
        category: str = None,
        after: str = None,
        before: str = None,
        begin: str = None,
        end: str = None,
        limit: str = None,
        use_current: bool = True,
    ):
        params = {
            k: v
            for k, v in {
                "instType": instType,
                "uly": uly,
                "instFamily": instFamily,
                "instId": instId,
                "ordType": ordType,
                "state": state,
                "category": category,
                "after": after,
                "before": before,
                "begin": begin,
                "end": end,
                "limit": limit,
            }.items()
            if v
        }

        current_endpoint = "/api/v5/trade/orders-history"
        history_endpoint = "/api/v5/trade/orders-history-archive"
        return await self._get(
            self.BASE_ENDPOINT + (current_endpoint if use_current else history_endpoint),
            params=params,
        )

    async def _get_bills_details(
        self,
        instType: str = None,
        ccy: str = None,
        mgnMode: str = None,
        ctType: str = None,
        type: str = None,
        subType: str = None,
        after: str = None,
        before: str = None,
        begin: str = None,
        end: str = None,
        limit: str = None,
        use_current: bool = True,
    ):
        params = {
            k: v
            for k, v in {
                "instType": instType,
                "ccy": ccy,
                "mgnMode": mgnMode,
                "ctType": ctType,
                "type": type,
                "subType": subType,
                "after": after,
                "before": before,
                "begin": begin,
                "end": end,
                "limit": limit,
            }.items()
            if v
        }

        current_endpoint = "/api/v5/account/bills"
        history_endpoint = "/api/v5/account/bills-archive"

        return await self._get(
            self.BASE_ENDPOINT + (current_endpoint if use_current else history_endpoint),
            params=params,
        )
