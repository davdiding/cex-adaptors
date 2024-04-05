from .exchanges.woo import WOOUnified
from .parsers.woo import WOOParser


class WOO(WOOUnified, WOOParser):
    def __init__(self):
        super().__init__()
        self.exchange_info = {}

    async def sync_exchange_info(self):
        self.exchange_info = await self.get_exchange_info()

    async def get_exchange_info(self) -> dict:
        self.exchange_info = await self._get_available_symbols()

        return self.exchange_info
