from typing import Literal, Optional

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

    async def get_tickers(self, market_type: Optional[Literal["spot", "margin", "futures", "perp"]] = None):

        results = {}

        tickers = ["spot", "linear", "inverse"]

        for _market_type in tickers:
            parsed_tickers = self.parser.parse_tickers(await self.bybit._get_tickers(_market_type), _market_type)
            id_map = self.parser.get_id_symbol_map(self.exchange_info, _market_type)

            for ticker in parsed_tickers:
                symbol = ticker["symbol"]
                if symbol not in id_map:
                    print(symbol)
                    continue
                id = id_map[symbol]

                results[id] = ticker

        if market_type:
            ids = self.parser.query_dict(self.exchange_info, {f"is_{market_type}": True})
            return self.parser.query_dict_by_keys(results, ids)
        else:
            return results
