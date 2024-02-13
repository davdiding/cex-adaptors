import unittest
from unittest import IsolatedAsyncioTestCase

from cex_services.bybit import Bybit

# from cex_services.okx import Okx


class TestBybit(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bybit = await Bybit.create()

    async def asyncTearDown(self):
        await self.bybit.close()

    async def test_get_exchange_info(self):
        response = await self.bybit.get_exchange_info()
        self.assertTrue(response)

    async def test_get_tickers(self):
        spot = await self.bybit.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.bybit.get_tickers("perp")
        self.assertTrue(perp)

        futures = await self.bybit.get_tickers("futures")
        self.assertTrue(futures)


if __name__ == "__main__":
    unittest.main()
