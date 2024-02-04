from .exchanges.gateio import GateioClient
from .parsers.gateio import GateioParser


class Gateio(object):
    name = "gateio"

    FUTURES_SETTLE = ["btc", "usdt", "usd"]

    def __init__(self):
        self.exchange = GateioClient()
        self.parser = GateioParser()
        self.exchange_info = {}

    async def close(self):
        await self.exchange.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self):
        spot = self.parser.parse_exchange_info(
            await self.exchange._get_currency_pairs(), self.parser.spot_exchange_info_parser
        )

        futures = {}
        for settle in self.FUTURES_SETTLE:
            future = self.parser.parse_exchange_info(
                await self.exchange._get_futures_info(settle),
                self.parser.futures_exchange_info_parser,
                settle=settle.upper(),
            )
            futures.update(future)

        return {**spot, **futures}
