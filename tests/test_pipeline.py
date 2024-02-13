import unittest
from unittest import IsolatedAsyncioTestCase

from cex_services.bybit import Bybit
from cex_services.okx import Okx


class TestBybit(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        bybit = await Bybit.create()
        response = await bybit.get_exchange_info()
        self.assertTrue(response)
        await bybit.close()
        return

    async def test_get_tickers(self):
        bybit = await Bybit.create()
        spot = await bybit.get_tickers("spot")
        self.assertTrue(spot)

        perp = await bybit.get_tickers("perp")
        self.assertTrue(perp)

        futures = await bybit.get_tickers("futures")
        self.assertTrue(futures)

        tickers = await bybit.get_tickers()
        self.assertTrue(tickers)
        await bybit.close()
        return


class TestOKX(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        okx = await Okx.create()
        response = await okx.get_exchange_info()
        self.assertTrue(response)
        await okx.close()
        return


if __name__ == "__main__":
    unittest.main()
