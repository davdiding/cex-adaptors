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

        inverse = self.parser.parse_exchange_info(
            await self.bitget._get_derivative_exchange_info("COIN-FUTURES"), self.parser.derivative_exchange_info_parser
        )

        linear_futures = {}
        for settle in self.parser.LINEAR_FUTURES_SETTLE:
            sub_linear = self.parser.parse_exchange_info(
                await self.bitget._get_derivative_exchange_info(f"{settle}-FUTURES"),
                self.parser.derivative_exchange_info_parser,
            )
            linear_futures.update(sub_linear)

        results = {**spot, **inverse, **linear_futures}
        if market_type:
            pass
        else:
            return results
