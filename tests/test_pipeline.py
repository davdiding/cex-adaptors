import tracemalloc
import unittest
from datetime import datetime as dt
from unittest import IsolatedAsyncioTestCase

from cex_services.bitget import Bitget
from cex_services.gateio import Gateio
from cex_services.htx import Htx
from cex_services.kucoin import Kucoin
from cex_services.okx import Okx
from cex_services.utils import query_dict

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


class TestKucoin(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.exchange = await Kucoin.create()

    async def asyncTearDown(self) -> None:
        await self.exchange.close()

    async def test_get_exchange_info(self):
        response = await self.exchange.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_klines(self):
        spot = await self.exchange.get_klines("BTC/USDT:USDT", "1d", num=120)
        self.assertEqual(len(spot), 120)

        futures = await self.exchange.get_klines("BTC/USD:USD-240329", "1d", num=23)
        self.assertEqual(len(futures), 23)

        perp = await self.exchange.get_klines("BTC/USDT:USDT-PERP", "1d", num=300)
        self.assertEqual(len(perp), 300)
        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        spot = await self.exchange.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEqual(len(spot), 30)

        futures = await self.exchange.get_klines("BTC/USD:USD-240329", "1d", start=start, end=end)
        self.assertEqual(len(futures), 31)

        perp = await self.exchange.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEqual(len(perp), 31)
        return


class TestGateio(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange = await Gateio.create()

    async def asyncTearDown(self):
        await self.exchange.close()

    async def test_get_exchange_info(self):
        response = await self.exchange.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_tickers(self):
        spot = await self.exchange.get_tickers("spot")
        self.assertTrue(spot)

        futures = await self.exchange.get_tickers("futures")
        self.assertTrue(futures)

        perp = await self.exchange.get_tickers("perp")
        self.assertTrue(perp)

        tickers = await self.exchange.get_tickers()
        self.assertTrue(tickers)
        return


class TestHtx(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange = await Htx.create()

    async def asyncTearDown(self):
        await self.exchange.close()

    async def test_get_exchange_info(self):
        response = await self.exchange.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_tickers(self):
        spot = await self.exchange.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.exchange.get_tickers("perp")
        self.assertTrue(perp)

        futures = await self.exchange.get_tickers("futures")
        self.assertTrue(futures)

        tickers = await self.exchange.get_tickers()
        self.assertTrue(tickers)
        return


class TestBitget(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange = await Bitget.create()

    async def asyncTearDown(self):
        await self.exchange.close()

    async def test_get_exchange_info(self):
        response = await self.exchange.get_exchange_info()
        self.assertTrue(response)
        return

    async def test_get_tickers(self):
        spot = await self.exchange.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.exchange.get_tickers("perp")
        self.assertTrue(perp)

        futures = await self.exchange.get_tickers("futures")
        self.assertTrue(futures)

        tickers = await self.exchange.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_klines_with_num(self):
        spot = await self.exchange.get_klines("BTC/USDT:USDT", "1d", num=333)
        self.assertEquals(len(spot), 333)

        instrument_id = list(query_dict(self.exchange.exchange_info, "is_futures == True").keys())[0]
        futures = await self.exchange.get_klines(instrument_id, "1m", num=333)
        self.assertEquals(len(futures), 333)

        perp = await self.exchange.get_klines("BTC/USDT:USDT-PERP", "5m", num=333)
        self.assertEquals(len(perp), 333)
        return

    async def test_get_klines_with_timestamp(self):
        start = int(dt.timestamp(dt(2024, 1, 1)) * 1000)
        end = int(dt.timestamp(dt(2024, 1, 31)) * 1000)

        spot = await self.exchange.get_klines("BTC/USDT:USDT", "1d", start=start, end=end)
        self.assertEquals(len(spot), 31)

        perp = await self.exchange.get_klines("BTC/USDT:USDT-PERP", "1d", start=start, end=end)
        self.assertEquals(len(perp), 31)

        instruments = list(query_dict(self.exchange.exchange_info, "is_futures == True").keys())[0]
        futures = await self.exchange.get_klines(instruments, "1d", start=start, end=end)
        self.assertEquals(len(futures), 31)
        return


if __name__ == "__main__":
    unittest.main()
