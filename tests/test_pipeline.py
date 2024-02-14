import tracemalloc
import unittest
from datetime import datetime as dt
from unittest import IsolatedAsyncioTestCase

# from cex_services.gateio import Gateio
# from cex_services.htx import Htx
# from cex_services.kucoin import Kucoin
from cex_services.okx import Okx

# from cex_services.binance import Binance


tracemalloc.start()


class TestOkx(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.okx = await Okx.create()

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

        futures = await self.okx.get_ticker("BTC/USD:BTC-240329")
        self.assertTrue(futures)
        return

    async def test_get_klines(self):
        self.okx = await self.okx.create()
        spot = await self.okx.get_klines("BTC/USDT:USDT", "1d", num=120)
        self.assertEqual(len(spot), 120)

        perp = await self.okx.get_klines("BTC/USDT:USDT-PERP", "1d", num=77)
        self.assertEqual(len(perp), 77)

        futures = await self.okx.get_klines("BTC/USD:BTC-240329", "1d", num=23)
        self.assertEqual(len(futures), 23)
        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        spot = await self.okx.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEqual(len(spot), 30)

        perp = await self.okx.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEqual(len(perp), 30)

        futures = await self.okx.get_klines("BTC/USD:BTC-240329", "1d", start=start, end=end)
        self.assertEqual(len(futures), 30)
        return


"""
class TestBinance(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.binance = await Binance.create()

    async def asyncTearDown(self):
        await self.binance.close()

    async def test_get_exchange_info(self):
        response = await self.binance.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_tickers(self):
        spot = await self.binance.get_tickers("spot")
        self.assertTrue(spot)

        futures = await self.binance.get_tickers("futures")
        self.assertTrue(futures)

        perp = await self.binance.get_tickers("perp")
        self.assertTrue(perp)

        tickers = await self.binance.get_tickers()
        self.assertTrue(tickers)

    async def test_get_ticker(self):
        spot = await self.binance.get_ticker("BTC/USDT:USDT")
        self.assertTrue(spot)

        perp = await self.binance.get_ticker("BTC/USDT:USDT-PERP")
        self.assertTrue(perp)

        futures = await self.binance.get_ticker("BTC/USD:BTC-240329")
        self.assertTrue(futures)
        return

    async def test_get_klines(self):
        spot = await self.binance.get_klines("BTC/USDT:USDT", "1d", num=120)
        self.assertEqual(len(spot), 120)

        perp = await self.binance.get_klines("BTC/USDT:USDT-PERP", "1d", num=77)
        self.assertEqual(len(perp), 77)

        futures = await self.binance.get_klines("BCH/USD:BCH-240628", "1d", num=23)
        self.assertEqual(len(futures), 23)
        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        spot = await self.binance.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEqual(len(spot), 30)

        perp = await self.binance.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEqual(len(perp), 30)

        futures = await self.binance.get_klines("BCH/USD:BCH-240628", "1d", start=start, end=end)
        self.assertEqual(len(futures), 30)
        return
"""

if __name__ == "__main__":
    unittest.main()
