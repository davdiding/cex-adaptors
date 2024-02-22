from .exchanges.bitget import BitgetUnified
from .parsers.bitget import BitgetParser
from .utils import query_dict


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
