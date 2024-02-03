from .exchanges.gateio import GateioClient
from .parsers.gateio import GateioParser


class Gateio(object):
    name = "gateio"

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
        spot = self.parser.parse_exchange_info(await self.exchange._get_currency_pairs(), None)

        return {**spot}
