from .exchanges.bybit import BybitUnified
from .parsers.bybit import BybitParser


class Bybit(object):
    name = "bybit"

    def __init__(self):
        self.bybit = BybitUnified()
        self.parser = BybitParser()
        self.exchange_info = {}

    async def close(self):
        await self.bybit.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        spot = self.parser.parse_exchange_info(
            await self.bybit._get_exchange_info("spot"), self.parser.spot_exchange_info_parser
        )
        linear = self.parser.parse_exchange_info(
            await self.bybit._get_exchange_info("linear"), self.parser.perp_futures_exchange_info_parser
        )
        inverse = self.parser.parse_exchange_info(
            await self.bybit._get_exchange_info("inverse"), self.parser.perp_futures_exchange_info_parser
        )

        return {**spot, **linear, **inverse}

    async def get_tickers(self) -> dict:

        results = {}

        tickers = ["spot", "linear", "inverse"]

        for market_type in tickers:
            parsed_tickers = self.parser.parse_tickers(await self.bybit._get_tickers(market_type), market_type)
            id_map = self.parser.get_id_symbol_map(self.exchange_info, market_type)

            for ticker in parsed_tickers:
                symbol = ticker["symbol"]
                if symbol not in id_map:
                    print(symbol)
                    continue
                id = id_map[symbol]

                results[id] = ticker

        return results
