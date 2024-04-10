from .exchanges.bitget import BitgetUnified
from .parsers.bitget import BitgetParser
from .utils import query_dict, sort_dict


class Bitget(BitgetUnified):
    name = "bitget"

    def __init__(self) -> None:
        super().__init__()
        self.parser = BitgetParser()
        self.exchange_info = {}

    async def sync_exchange_info(self):
        self.exchange_info = await self.get_exchange_info()

    async def get_exchange_info(self, market_type: str = None):
        if market_type:
            if market_type == "spot":
                return self.parser.parse_exchange_info(
                    await self._get_spot_exchange_info(), self.parser.spot_exchange_info_parser
                )
            else:
                derivative = {}
                inverse = self.parser.parse_exchange_info(
                    await self._get_derivative_exchange_info("COIN-FUTURES"),
                    self.parser.derivative_exchange_info_parser,
                )
                derivative.update(inverse)

                for settle in self.parser.LINEAR_FUTURES_SETTLE:
                    sub_linear = self.parser.parse_exchange_info(
                        await self._get_derivative_exchange_info(f"{settle}-FUTURES"),
                        self.parser.derivative_exchange_info_parser,
                    )
                    derivative.update(sub_linear)

                instrument_id = list(query_dict(self.exchange_info, f"is_{market_type} == True").keys())
                return {k: v for k, v in derivative.items() if k in instrument_id}
        else:
            spot = self.parser.parse_exchange_info(
                await self._get_spot_exchange_info(), self.parser.spot_exchange_info_parser
            )

            inverse = self.parser.parse_exchange_info(
                await self._get_derivative_exchange_info("COIN-FUTURES"),
                self.parser.derivative_exchange_info_parser,
            )

            linear_futures = {}
            for settle in self.parser.LINEAR_FUTURES_SETTLE:
                sub_linear = self.parser.parse_exchange_info(
                    await self._get_derivative_exchange_info(f"{settle}-FUTURES"),
                    self.parser.derivative_exchange_info_parser,
                )
                linear_futures.update(sub_linear)

            return {**spot, **inverse, **linear_futures}

    async def get_tickers(self, market_type: str = None):
        if market_type:
            if market_type == "spot":
                return self.parser.parse_tickers(await self._get_spot_tickers(), self.exchange_info, "spot")
            else:
                derivative = {}
                inverse = self.parser.parse_tickers(
                    await self._get_derivative_tickers("COIN-FUTURES"), self.exchange_info, "derivative"
                )
                derivative.update(inverse)

                for settle in self.parser.LINEAR_FUTURES_SETTLE:
                    sub_linear = self.parser.parse_tickers(
                        await self._get_derivative_tickers(f"{settle}-FUTURES"), self.exchange_info, "derivative"
                    )
                    derivative.update(sub_linear)

                instrument_id = list(query_dict(self.exchange_info, f"is_{market_type} == True").keys())
                return {k: v for k, v in derivative.items() if k in instrument_id}
        else:
            spot = self.parser.parse_tickers(await self._get_spot_tickers(), self.exchange_info, "spot")
            inverse = self.parser.parse_tickers(
                await self._get_derivative_tickers("COIN-FUTURES"), self.exchange_info, "derivative"
            )
            linear_futures = {}
            for settle in self.parser.LINEAR_FUTURES_SETTLE:
                sub_linear = self.parser.parse_tickers(
                    await self._get_derivative_tickers(f"{settle}-FUTURES"), self.exchange_info, "derivative"
                )
                linear_futures.update(sub_linear)
            return {**spot, **inverse, **linear_futures}

    async def get_ticker(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise f"{instrument_id} not found in {self.name} exchange info"

        info = self.exchange_info[instrument_id]
        _symbol = info["raw_data"]["symbol"]
        market_type = self.parser.get_market_type(info)
        product_type = self.parser.get_product_type(info)

        method_map = {
            "spot": self._get_spot_tickers,
            "derivative": self._get_derivative_ticker,
        }
        params = {"symbol": _symbol} if market_type == "spot" else {"symbol": _symbol, "productType": product_type}
        return {instrument_id: self.parser.parse_raw_ticker(await method_map[market_type](**params), info, market_type)}

    async def get_last_price(self, instrument_id: str) -> dict:
        ticker = await self.get_ticker(instrument_id)
        return {
            "timestamp": ticker["timestamp"],
            "instrument_id": instrument_id,
            "last_price": ticker["last"],
            "raw_data": ticker["raw_data"],
        }

    async def get_index_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise f"{instrument_id} not found in {self.name} exchange info"

        info = self.exchange_info[instrument_id]
        if info["is_spot"]:
            raise ValueError(f"{instrument_id} is not a derivative instrument")
        product_type = self.parser.get_product_type(info)
        _symbol = info["raw_data"]["symbol"]
        return self.parser.parse_mark_index_price(
            await self._get_derivative_mark_index_price(_symbol, product_type), info, "index"
        )

    async def get_mark_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise f"{instrument_id} not found in {self.name} exchange info"

        info = self.exchange_info[instrument_id]
        if info["is_spot"]:
            raise ValueError(f"{instrument_id} is not a derivative instrument")
        product_type = self.parser.get_product_type(info)
        _symbol = info["raw_data"]["symbol"]
        return self.parser.parse_mark_index_price(
            await self._get_derivative_mark_index_price(_symbol, product_type), info, "mark"
        )

    async def get_orderbook(self, instrument_id: str, depth: int = None) -> dict:
        if instrument_id not in self.exchange_info:
            raise f"{instrument_id} not found in {self.name} exchange info"

        info = self.exchange_info[instrument_id]
        _symbol = info["raw_data"]["symbol"]
        _market_type = self.parser.get_market_type(info)
        limit = "max"
        params = {
            "symbol": _symbol,
            "limit": limit,
        }

        if _market_type == "derivative":
            params.update({"productType": self.parser.get_product_type(info)})
        method_map = {
            "spot": self._get_spot_merge_depth,
            "derivative": self._get_derivative_merge_market_depth,
        }
        orderbook = self.parser.parse_orderbook(await method_map[_market_type](**params), info)

        if depth:
            orderbook["asks"] = sorted(orderbook["asks"], key=lambda x: x["price"], reverse=False)[:depth]
            orderbook["bids"] = sorted(orderbook["bids"], key=lambda x: x["price"], reverse=True)[:depth]

        return orderbook

    async def get_klines(
        self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None
    ) -> dict:
        if instrument_id not in self.exchange_info:
            return {}

        market_type = self.parser.get_market_type(self.exchange_info[instrument_id])
        _symbol = self.exchange_info[instrument_id]["raw_data"]["symbol"]
        _interval = self.parser.get_interval(interval, market_type)
        limit = 300

        method_map = {
            "spot": self._get_spot_candlesticks,
            "derivative": self._get_derivative_candlesticks,
        }

        params = {
            "symbol": _symbol,
            "granularity": _interval,
            "limit": limit,
        }
        if market_type == "derivative":
            params.update({"productType": self.parser.get_product_type(self.exchange_info[instrument_id])})

        results = {}
        query_end = None
        if start and end:
            query_end = end
            while True:
                params.update({"endTime": query_end})

                result = self.parser.parse_klines(await method_map[market_type](**params), market_type)
                results.update(result)

                if not result or len(result) < limit:
                    break

                query_end = sorted(list(result.keys()))[0]

                if query_end < start:
                    break

            return {k: v for k, v in results.items() if start <= k <= end}
        elif num:
            while True:
                params.update({"endTime": query_end})

                result = self.parser.parse_klines(await method_map[market_type](**params), market_type)
                results.update(result)

                if not result or len(result) < limit:
                    break

                query_end = sorted(list(result.keys()))[0]

                if len(results) >= num:
                    break
            return sort_dict(results, ascending=True, num=num)

        else:
            raise ValueError("(start, end) or num must be provided")

    async def get_current_funding_rate(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise f"{instrument_id} not found in {self.name} exchange info"

        info = self.exchange_info[instrument_id]
        _symbol = info["raw_data"]["symbol"]
        _product_type = self.parser.get_product_type(info)

        return self.parser.parse_current_funding_rate(
            await self._get_derivative_current_funding_rate(_symbol, _product_type), info
        )

    async def get_history_funding_rate(
        self, instrument_id: str, start: int = None, end: int = None, num: int = 30
    ) -> list:
        if instrument_id not in self.exchange_info:
            raise f"{instrument_id} not found in {self.name} exchange info"

        info = self.exchange_info[instrument_id]
        _symbol = info["raw_data"]["symbol"]
        _product_type = self.parser.get_product_type(info)
        limit = 100
        page = 1
        params = {"symbol": _symbol, "productType": _product_type, "pageSize": limit, "pageNo": page}

        results = []
        if start and end:
            while True:
                params.update({"pageNo": page})
                result = self.parser.parse_history_funding_rate(
                    await self._get_derivative_history_funding_rate(**params), info
                )
                results.extend(result)

                # exclude data with same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < limit:
                    break
                min_timestamp = min([i["timestamp"] for i in result])
                if min_timestamp < start:
                    break

            return sorted(
                [i for i in results if start <= i["timestamp"] <= end], key=lambda x: x["timestamp"], reverse=False
            )
        elif num:
            while True:
                params.update({"pageNo": page})
                result = self.parser.parse_history_funding_rate(
                    await self._get_derivative_history_funding_rate(**params), info
                )
                results.extend(result)

                # exclude data with same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < limit or len(results) > num:
                    break
                page += 1
                continue
            return sorted(results, key=lambda x: x["timestamp"], reverse=False)[-num:]
        else:
            raise ValueError("(start, end) or num must be provided")
