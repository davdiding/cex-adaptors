from .base import BaseClient


class BybitUnified(BaseClient):
    name = "bybit"
    BASE_ENDPOINT = "https://api.bybit.com"
    TESTNET_BASE_ENDPOINT = "https://api-testnet.bybit.com"

    def __init__(self, api_key: str = None, api_secret: str = None, testnet: bool = False):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT if not testnet else self.TESTNET_BASE_ENDPOINT

        self.auth_data = {
            "api_key": api_key,
            "api_secret": api_secret,
        }
        self.testnet = testnet

    async def _get_exchange_info(self, category: str) -> dict:
        return await self._get(self.base_endpoint + "/v5/market/instruments-info", params={"category": category})

    async def _get_tickers(self, category: str) -> dict:
        return await self._get(self.base_endpoint + "/v5/market/tickers", params={"category": category})

    async def _get_ticker(self, symbol: str, category: str):
        params = {"symbol": symbol, "category": category}
        return await self._get(self.base_endpoint + "/v5/market/tickers", params=params)

    async def _get_klines(
        self, symbol: str, interval: str, category: str = None, start: int = None, end: int = None, limit: int = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "interval": interval,
                "start": start,
                "end": end,
                "limit": limit,
            }.items()
            if v is not None
        }

        return await self._get(self.base_endpoint + "/v5/market/kline", params=params)

    async def _get_funding_rate(
        self, category: str, symbol: str, startTime: int = None, endTime: int = None, limit: int = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "start_time": startTime,
                "end_time": endTime,
                "limit": limit,
            }.items()
            if v is not None
        }

        return await self._get(self.base_endpoint + "/v5/market/funding/history", params=params)

    async def _get_open_interest(
        self, category: str, symbol: str, interval: str, startTime: int = None, endTime: int = None, limit: int = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "intervalTime": interval,
                "start_time": startTime,
                "end_time": endTime,
                "limit": limit,
            }.items()
            if v
        }

        return await self._get(self.base_endpoint + "/v5/market/open-interest", params=params)

    async def _get_orderbook(self, category: str, symbol: str, limit: int = None):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "limit": limit,
            }.items()
            if v
        }

        return await self._get(self.base_endpoint + "/v5/market/orderbook", params=params)

    # Private endpoint
    async def _get_wallet_balance(self, accountType: str = "UNIFIED", coin: str = None):
        params = {
            k: v
            for k, v in {
                "accountType": accountType,
                "coin": coin,
            }.items()
            if v
        }

        return await self._get(
            self.base_endpoint + "/v5/account/wallet-balance", params=params, auth_data=self.auth_data
        )

    async def _get_position_info(self, category: str, symbol: str = None, limit: int = None):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "limit": limit,
            }.items()
            if v
        }

        return await self._get(self.base_endpoint + "/v5/position/list", params=params, auth_data=self.auth_data)

    async def _place_order(
        self, category: str, symbol: str, side: str, orderType: str, qty: str, price: str = None, marketUnit: str = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "side": side,
                "orderType": orderType,
                "qty": qty,
                "price": price,
                "marketUnit": marketUnit,
            }.items()
            if v
        }

        return await self._post(self.base_endpoint + "/v5/order/create", params=params, auth_data=self.auth_data)

    async def _get_history_order(
        self,
        category: str,
        symbol: str = None,
        baseCoin: str = None,
        settleCoin: str = None,
        orderId: str = None,
        orderLinkId: str = None,
        orderFilter: str = None,
        orderStatus: str = None,
        startTime: int = None,
        endTime: int = None,
        limit: int = None,
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "baseCoin": baseCoin,
                "settleCoin": settleCoin,
                "orderId": orderId,
                "orderLinkId": orderLinkId,
                "orderFilter": orderFilter,
                "orderStatus": orderStatus,
                "startTime": startTime,
                "endTime": endTime,
                "limit": limit,
            }.items()
            if v
        }
        return await self._get(self.base_endpoint + "/v5/order/history", params=params, auth_data=self.auth_data)

    async def _get_opened_order(
        self,
        category: str,
        symbol: str = None,
        baseCoin: str = None,
        settleCoin: str = None,
        orderId: str = None,
        orderLinkId: str = None,
        openOnly: int = None,
        orderFilter: str = None,
        limit: int = None,
        cursor: str = None,
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "baseCoin": baseCoin,
                "settleCoin": settleCoin,
                "orderId": orderId,
                "orderLinkId": orderLinkId,
                "openOnly": openOnly,
                "orderFilter": orderFilter,
                "limit": limit,
                "cursor": cursor,
            }.items()
            if v
        }

        return await self._get(self.base_endpoint + "/v5/order/realtime", params=params, auth_data=self.auth_data)

    async def _cancel_order(
        self, category: str, symbol: str, orderId: str = None, orderLinkId: str = None, orderFilter: str = None
    ):
        params = {
            k: v
            for k, v in {
                "category": category,
                "symbol": symbol,
                "orderId": orderId,
                "orderLinkId": orderLinkId,
                "orderFilter": orderFilter,
            }.items()
            if v
        }

        return await self._post(self.base_endpoint + "/v5/order/cancel", params=params, auth_data=self.auth_data)

    async def _get_all_coins_balance(
        self, accountType: str = "UNIFIED", memberId: str = None, coin: str = None, withBonus: int = None
    ):
        params = {
            k: v
            for k, v in {
                "accountType": accountType,
                "memberId": memberId,
                "coin": coin,
                "withBonus": withBonus,
            }.items()
            if v
        }

        return await self._get(
            self.base_endpoint + "/v5/asset/transfer/query-account-coins-balance",
            params=params,
            auth_data=self.auth_data,
        )
