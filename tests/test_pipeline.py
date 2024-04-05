import tracemalloc
import unittest
from datetime import datetime as dt
from unittest import IsolatedAsyncioTestCase

from cex_adaptors.okx import Okx

tracemalloc.start()


class TestOkx(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.okx = Okx()
        await self.okx.sync_exchange_info()

    async def asyncTearDown(self):
        await self.okx.close()

    async def test_get_exchange_info(self):
        response = await self.okx.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_tickers(self):
        spot = await self.okx.get_tickers("spot")
        self.assertTrue(spot)

        futures = await self.okx.get_tickers("futures")
        self.assertTrue(futures)

        perp = await self.okx.get_tickers("perp")
        self.assertTrue(perp)

        tickers = await self.okx.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_ticker(self):
        spot = await self.okx.get_ticker("BTC/USDT:USDT")
        self.assertTrue(spot)

        perp = await self.okx.get_ticker("BTC/USDT:USDT-PERP")
        self.assertTrue(perp)

        return

    async def test_get_klines(self):
        spot = await self.okx.get_klines("BTC/USDT:USDT", "1d", num=120)
        self.assertEqual(len(spot), 120)

        perp = await self.okx.get_klines("BTC/USDT:USDT-PERP", "1d", num=77)
        self.assertEqual(len(perp), 77)

        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        spot = await self.okx.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEqual(len(spot), 30)

        perp = await self.okx.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEqual(len(perp), 30)

        return

    async def test_get_current_funding_rate(self):
        funding_rate = await self.okx.get_current_funding_rate("BTC/USDT:USDT-PERP")
        self.assertTrue(funding_rate)
        return

    async def test_get_history_funding_rate(self):
        history_funding_rate = await self.okx.get_history_funding_rate("BTC/USDT:USDT-PERP", num=30)
        self.assertEqual(len(history_funding_rate), 30)

        start = int(dt.timestamp(dt(2024, 3, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 3, 3)) * 1000)
        history_funding_rate = await self.okx.get_history_funding_rate("BTC/USDT:USDT-PERP", start=start, end=end)
        self.assertEqual(len(history_funding_rate), 7)
        return


if __name__ == "__main__":
    unittest.main()
