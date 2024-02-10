from datetime import datetime as dt
from unittest import IsolatedAsyncioTestCase

# from cex_services.binance import Binance
# from cex_services.bybit import Bybit
# from cex_services.gateio import Gateio
# from cex_services.htx import Htx
from cex_services.okx import Okx


class TestOkx(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        okx = await Okx.create()
        response = await okx.get_exchange_info()
        self.assertTrue(response)

        await okx.close()
        return

    async def test_get_tickers(self):
        okx = await Okx.create()
        spot = await okx.get_tickers("spot")
        self.assertTrue(spot)

        perp = await okx.get_tickers("perp")
        self.assertTrue(perp)

        futures = await okx.get_tickers("futures")
        self.assertTrue(futures)
        await okx.close()
        return

    async def test_get_ticker(self):
        okx = await Okx.create()
        spot = await okx.get_ticker("BTC/USDT:USDT")
        self.assertTrue(spot)

        perp = await okx.get_ticker("BTC/USDT:USDT-PERP")
        self.assertTrue(perp)

        futures = await okx.get_ticker("BTC/USD:BTC-240329")
        self.assertTrue(futures)
        await okx.close()
        return

    async def test_get_klines(self):
        okx = await Okx.create()
        spot = await okx.get_klines("BTC/USDT:USDT", "1d", num=120)
        self.assertEquals(len(spot), 120)

        perp = await okx.get_klines("BTC/USDT:USDT-PERP", "1d", num=77)
        self.assertEquals(len(perp), 77)

        futures = await okx.get_klines("BTC/USD:BTC-240329", "1d", num=23)
        self.assertEquals(len(futures), 23)
        await okx.close()
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
        await okx.close()
        return


if __name__ == "__main__":
    import unittest

    unittest.main()
