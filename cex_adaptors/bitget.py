from .exchanges.bitget import BitgetUnified
from .parsers.bitget import BitgetParser
from .utils import query_dict, sort_dict


class Bitget(object):
    def __init__(self) -> None:
        self.bitget = BitgetUnified()
        self.parser = BitgetParser()
        self.exchange_info = {}

    async def close(self):
        await self.bitget.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        if market_type:
            if market_type == "spot":
                return self.parser.parse_exchange_info(
                    await self.bitget._get_spot_exchange_info(), self.parser.spot_exchange_info_parser
                )
            else:
                derivative = {}
                inverse = self.parser.parse_exchange_info(
                    await self.bitget._get_derivative_exchange_info("COIN-FUTURES"),
                    self.parser.derivative_exchange_info_parser,
                )
                derivative.update(inverse)

                for settle in self.parser.LINEAR_FUTURES_SETTLE:
                    sub_linear = self.parser.parse_exchange_info(
                        await self.bitget._get_derivative_exchange_info(f"{settle}-FUTURES"),
                        self.parser.derivative_exchange_info_parser,
                    )
                    derivative.update(sub_linear)

                instrument_id = list(query_dict(self.exchange_info, f"is_{market_type} == True").keys())
                return {k: v for k, v in derivative.items() if k in instrument_id}
        else:
            spot = self.parser.parse_exchange_info(
                await self.bitget._get_spot_exchange_info(), self.parser.spot_exchange_info_parser
            )

            inverse = self.parser.parse_exchange_info(
                await self.bitget._get_derivative_exchange_info("COIN-FUTURES"),
                self.parser.derivative_exchange_info_parser,
            )

            linear_futures = {}
            for settle in self.parser.LINEAR_FUTURES_SETTLE:
                sub_linear = self.parser.parse_exchange_info(
                    await self.bitget._get_derivative_exchange_info(f"{settle}-FUTURES"),
                    self.parser.derivative_exchange_info_parser,
                )
                linear_futures.update(sub_linear)

            return {**spot, **inverse, **linear_futures}

    async def get_tickers(self, market_type: str = None):
        if market_type:
            if market_type == "spot":
                return self.parser.parse_tickers(await self.bitget._get_spot_tickers(), self.exchange_info, "spot")
            else:
                derivative = {}
                inverse = self.parser.parse_tickers(
                    await self.bitget._get_derivative_tickers("COIN-FUTURES"), self.exchange_info, "derivative"
                )
                derivative.update(inverse)

                for settle in self.parser.LINEAR_FUTURES_SETTLE:
                    sub_linear = self.parser.parse_tickers(
                        await self.bitget._get_derivative_tickers(f"{settle}-FUTURES"), self.exchange_info, "derivative"
                    )
                    derivative.update(sub_linear)

                instrument_id = list(query_dict(self.exchange_info, f"is_{market_type} == True").keys())
                return {k: v for k, v in derivative.items() if k in instrument_id}
        else:
            spot = self.parser.parse_tickers(await self.bitget._get_spot_tickers(), self.exchange_info, "spot")
            inverse = self.parser.parse_tickers(
                await self.bitget._get_derivative_tickers("COIN-FUTURES"), self.exchange_info, "derivative"
            )
            linear_futures = {}
            for settle in self.parser.LINEAR_FUTURES_SETTLE:
                sub_linear = self.parser.parse_tickers(
                    await self.bitget._get_derivative_tickers(f"{settle}-FUTURES"), self.exchange_info, "derivative"
                )
                linear_futures.update(sub_linear)
            return {**spot, **inverse, **linear_futures}

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
            "spot": self.bitget._get_spot_candlesticks,
            "derivative": self.bitget._get_derivative_candlesticks,
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
            return {"code": 400, "msg": "(start, end) or (num) must be provided at least one."}
