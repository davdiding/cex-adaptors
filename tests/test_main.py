from datetime import datetime as dt
from unittest import IsolatedAsyncioTestCase

from cex_services.binance import Binance
from cex_services.bybit import Bybit
from cex_services.gateio import Gateio
from cex_services.htx import Htx
from cex_services.okx import Okx


class TestBybit(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        bybit = await Bybit.create()
        response = await bybit.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_kines(self):
        bybit = await Bybit.create()
        spot = await bybit.get_klines("BTC/USDT:USDT", "1d", num=333)
        self.assertEquals(len(spot), 333)

        perp = await bybit.get_klines("BTC/USDT:USDT-PERP", "1d", num=222)
        self.assertEquals(len(perp), 222)

        futures = await bybit.get_klines("BTC/USD:BTC-240329", "1d", num=23)
        self.assertEquals(len(futures), 23)
        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        bybit = await Bybit.create()
        spot = await bybit.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEquals(len(spot), 30)

        perp = await bybit.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEquals(len(perp), 30)

        return


class TestGateio(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        gateio = await Gateio.create()
        response = await gateio.get_exchange_info()
        self.assertTrue(response)
        return


class TestHtx(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        htx = await Htx.create()
        response = await htx.get_exchange_info()
        self.assertTrue(response)
        return


class TestBinance(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        binance = await Binance.create()
        response = await binance.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_ticker(self):
        binance = await Binance.create()
        spot = await binance.get_ticker("BTC/USDT:USDT")
        self.assertTrue(spot)

        perp = await binance.get_ticker("BTC/USDT:USDT-PERP")
        self.assertTrue(perp)

        futures = await binance.get_ticker("BCH/USD:BCH-240628")
        self.assertTrue(futures)
        return

    async def test_get_klines(self):
        binance = await Binance.create()
        spot = await binance.get_klines("BTC/USDT:USDT", "1d", num=1333)
        self.assertEquals(len(spot), 1333)

        perp = await binance.get_klines("BTC/USDT:USDT-PERP", "1d", num=666)
        self.assertEquals(len(perp), 666)

        futures = await binance.get_klines("BCH/USD:BCH-240628", "1d", num=23)
        self.assertEquals(len(futures), 23)
        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        binance = await Binance.create()
        spot = await binance.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEquals(len(spot), 30)

        perp = await binance.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEquals(len(perp), 30)

        futures = await binance.get_klines("BCH/USD:BCH-240628", "1d", start=start, end=end)
        self.assertEquals(len(futures), 30)
        return


class TestOkx(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        okx = await Okx.create()
        response = await okx.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_tickers(self):
        okx = await Okx.create()
        spot = await okx.get_tickers("spot")
        self.assertTrue(spot)

        perp = await okx.get_tickers("perp")
        self.assertTrue(perp)

        futures = await okx.get_tickers("futures")
        self.assertTrue(futures)
        return

    async def test_get_ticker(self):
        okx = await Okx.create()
        spot = await okx.get_ticker("BTC/USDT:USDT")
        self.assertTrue(spot)

        perp = await okx.get_ticker("BTC/USDT:USDT-PERP")
        self.assertTrue(perp)

        futures = await okx.get_ticker("BTC/USD:BTC-240329")
        self.assertTrue(futures)
        return

    async def test_get_klines(self):
        okx = await Okx.create()
        spot = await okx.get_klines("BTC/USDT:USDT", "1d", num=120)
        self.assertEquals(len(spot), 120)

        perp = await okx.get_klines("BTC/USDT:USDT-PERP", "1d", num=77)
        self.assertEquals(len(perp), 77)

        futures = await okx.get_klines("BTC/USD:BTC-240329", "1d", num=23)
        self.assertEquals(len(futures), 23)
        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        okx = await Okx.create()
        spot = await okx.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEquals(len(spot), 30)

        perp = await okx.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEquals(len(perp), 30)

        futures = await okx.get_klines("BTC/USD:BTC-240329", "1d", start=start, end=end)
        self.assertEquals(len(futures), 30)
        return


if __name__ == "__main__":
    import unittest

    unittest.main()
