from .exchanges.kucoin import KucoinFutures, KucoinSpot
from .parsers.kucoin import KucoinParser


class Kucoin(object):
    name = "kucoin"

    def __init__(self):
        self.spot = KucoinSpot()
        self.futures = KucoinFutures()
        self.parser = KucoinParser()

        self.exchange_info = {}

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def close(self):
        await self.spot.close()
        await self.futures.close()

    async def get_exchange_info(self) -> dict:
        spot = self.parser.parse_exchange_info(
            await self.spot._get_symbol_list(), self.parser.spot_exchange_info_parser
        )
        futures = self.parser.parse_exchange_info(
            await self.futures._get_symbol_list(), self.parser.futures_exchange_info_parser
        )

        return {**spot, **futures}

    async def get_tickers(self, market_type: str = None) -> dict:
        if market_type == "spot":
            return self.parser.parse_spot_tickers(await self.spot._get_tickers())
        elif market_type == "futures":
            pass
        else:
            pass
