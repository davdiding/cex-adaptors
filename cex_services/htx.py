from .exchanges.htx import HtxFutures, HtxUnified
from .parsers.htx import HtxParser


class Htx(object):
    name = "htx"

    def __init__(self):
        self.spot = HtxUnified()
        self.futures = HtxFutures()
        self.parser = HtxParser()
        self.exchange_info = {}

    async def close(self):
        await self.spot.close()
        await self.futures.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        spot = self.parser.parse_exchange_info(
            await self.spot._get_exchange_info(), self.parser.spot_exchange_info_parser
        )
        linear = self.parser.parse_exchange_info(
            await self.futures._get_linear_contract_info(), self.parser.linear_exchange_info_parser
        )
        inverse_futures = self.parser.parse_exchange_info(
            await self.futures._get_inverse_futures_info(), self.parser.inverse_futures_exchange_info_parser
        )
        inverse_perp = self.parser.parse_exchange_info(
            await self.futures._get_inverse_perp_info(), self.parser.inverse_perp_exchange_info_parser
        )

        return {**spot, **linear, **inverse_futures, **inverse_perp}
