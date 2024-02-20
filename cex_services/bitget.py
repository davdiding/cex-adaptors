from .exchanges.biget import BitgetUnified
from .parsers.biget import BitgetParser


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
        spot = self.parser.parse_exchange_info(
            await self.bitget._get_spot_exchange_info(), self.parser.spot_exchange_info_parser
        )

        results = {**spot}
        if market_type:
            pass
        else:
            return results
