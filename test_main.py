import os
from datetime import datetime as dt
from datetime import timedelta as td
from unittest import IsolatedAsyncioTestCase

from dotenv import load_dotenv

from cex_adaptors.binance import Binance
from cex_adaptors.bitget import Bitget
from cex_adaptors.bybit import Bybit
from cex_adaptors.htx import Htx
from cex_adaptors.kucoin import Kucoin
from cex_adaptors.okx import Okx

load_dotenv()


def get_yesterday_timestamp():
    date = dt.date(dt.now()) - td(days=1)
    return int(dt(date.year, date.month, date.day).timestamp() * 1000)


class TestOkx(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange = Okx(
            api_key=os.getenv("OKX_API_KEY_DEMO"),
            api_secret=os.getenv("OKX_API_SECRET_DEMO"),
            passphrase=os.getenv("OKX_API_PASSPHRASE_DEMO"),
            flag="1",
        )
        await self.exchange.sync_exchange_info()

    async def asyncTearDown(self):
        await self.exchange.close()

    async def test_exchange_info(self):
        spot = await self.exchange.get_exchange_info("spot")
        self.assertTrue(spot is not None)

        futures = await self.exchange.get_exchange_info("futures")
        self.assertTrue(futures is not None)

        perp = await self.exchange.get_exchange_info("perp")
        self.assertTrue(perp is not None)

        exchange_info = await self.exchange.get_exchange_info()
        self.assertTrue(exchange_info is not None)
        return

    # Private endpoints
    async def test_get_account_info(self):
        account = await self.exchange.get_account_info()
        self.assertTrue(account)
        return

    async def test_get_balance(self):
        balance = await self.exchange.get_balance()
        self.assertTrue(balance)
        return

    async def test_get_positions(self):
        position = await self.exchange.get_positions()
        self.assertTrue(position)
        return

    async def test_place_market_order(self):
        instrument_id = "ADA/USDT:USDT"
        side = "buy"
        volume = 1000
        order = await self.exchange.place_market_order(instrument_id, side, volume)
        self.assertTrue(order)
        return

    async def test_place_limit_order(self):
        instrument_id = "ADA/USDT:USDT"
        side = "buy"
        price = 0.5
        volume = 1000
        order = await self.exchange.place_limit_order(instrument_id, side, price, volume)

        cancel = await self.exchange.cancel_order(instrument_id, order["order_id"])
        self.assertTrue(cancel)
        return

    async def test_get_funding_rate_with_timestamp(self):
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.exchange.exchange_info if self.exchange.exchange_info[k]["is_futures"]][0]

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1

        for i in [perp, futures]:
            funding_rate = await self.exchange.get_history_funding_rate(i, start=start, end=end)
            self.assertEqual(len(funding_rate), target_num, f"{i} {len(funding_rate)}")

        return


class TestBinance(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.binance = Binance(
            api_key=os.getenv("BINANCE_API_KEY"),
            api_secret=os.getenv("BINANCE_API_SECRET"),
        )
        await self.binance.sync_exchange_info()

    async def asyncTearDown(self):
        await self.binance.close()

    async def test_exchange_info(self):
        spot = await self.binance.get_exchange_info("spot")
        self.assertTrue(spot is not None)

        futures = await self.binance.get_exchange_info("futures")
        self.assertTrue(futures is not None)

        perp = await self.binance.get_exchange_info("perp")
        self.assertTrue(perp is not None)

        exchange_info = await self.binance.get_exchange_info()
        self.assertTrue(exchange_info is not None)
        return

    # Private test
    async def test_get_account_info(self):
        account = await self.binance.get_spot_account_info()
        self.assertTrue(account)
        return

    async def test_get_funding_rate_with_num(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        num = 1333
        funding_rate = await self.binance.get_history_funding_rate(instrument_id, num=num)
        self.assertEqual(len(funding_rate), num)

        instrument_id = "BTC/USD:BTC-PERP"
        num = 1333
        funding_rate = await self.binance.get_history_funding_rate(instrument_id, num=num)
        self.assertEqual(len(funding_rate), num)
        return

    async def test_get_funding_rate_with_timestamp(self):
        # linear
        instrument_id = "BTC/USDT:USDT-PERP"

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1

        funding_rate = await self.binance.get_history_funding_rate(instrument_id, start=start, end=end)
        self.assertEqual(len(funding_rate), target_num)

        instrument_id = "BTC/USD:BTC-PERP"
        funding_rate = await self.binance.get_history_funding_rate(instrument_id, start=start, end=end)
        self.assertEqual(len(funding_rate), target_num)
        return

    async def test_get_price(self):
        # define instrument
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_futures"]][0]
        inverse = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_inverse"]][0]

        # get last price
        for instrument_id in [spot, perp, futures, inverse]:
            last_price = await self.binance.get_last_price(instrument_id)
            self.assertTrue(last_price)

        # get index price
        for instrument_id in [spot, perp, futures, inverse]:
            index_price = await self.binance.get_index_price(instrument_id)
            self.assertTrue(index_price)

        # get mark price
        for instrument_id in [perp, futures, inverse]:
            mark_price = await self.binance.get_mark_price(instrument_id)
            self.assertTrue(mark_price)

        return

    async def test_get_open_interest(self):
        linear = "BTC/USDT:USDT-PERP"
        inverse = "BTC/USD:BTC-PERP"

        for i in [linear, inverse]:
            oi = await self.binance.get_open_interest(i)
            self.assertTrue(oi)
        return

    async def test_get_orderbook(self):
        # define instrument
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_futures"]][0]
        inverse = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_inverse"]][0]

        depth = 333

        # get orderbook
        for instrument_id in [spot, perp, futures, inverse]:
            orderbook = await self.binance.get_orderbook(instrument_id, depth)

            # check if orderbook depth is correct
            self.assertEqual(len(orderbook["asks"]), depth)
            self.assertEqual(len(orderbook["bids"]), depth)
        return


class TestHTX(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.htx = Htx()
        await self.htx.sync_exchange_info()

    async def asyncTearDown(self):
        await self.htx.close()

    async def test_get_tickers(self):
        spot = await self.htx.get_tickers("spot")
        self.assertTrue(spot)

        futures = await self.htx.get_tickers("futures")
        self.assertTrue(futures)

        perp = await self.htx.get_tickers("perp")
        self.assertTrue(perp)

        tickers = await self.htx.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_ticker(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.htx.exchange_info if self.htx.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, futures]:
            ticker = await self.htx.get_ticker(i)
            self.assertTrue(ticker)
        return


class TestKucoin(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.kucoin_public = Kucoin()
        await self.kucoin_public.sync_exchange_info()

    async def asyncTearDown(self):
        await self.kucoin_public.close()

    async def test_get_tickers(self):
        spot = await self.kucoin_public.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.kucoin_public.get_tickers("perp")
        self.assertTrue(perp)

        future = await self.kucoin_public.get_tickers("futures")
        self.assertTrue(future)

        tickers = await self.kucoin_public.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_ticker(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            ticker = await self.kucoin_public.get_ticker(i)
            self.assertTrue(ticker)
        return

    async def test_get_last_price(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            last_price = await self.kucoin_public.get_last_price(i)
            self.assertTrue(last_price)
        return

    async def test_get_mark_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            mark_price = await self.kucoin_public.get_mark_price(i)
            self.assertTrue(mark_price)
        return

    async def test_get_index_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            index_price = await self.kucoin_public.get_index_price(i)
            self.assertTrue(index_price)
        return

    async def test_get_orderbook(self):
        perp = "BTC/USDT:USDT-PERP"
        depth = 222

        orderbook = await self.kucoin_public.get_orderbook(perp, depth)

        self.assertEqual(len(orderbook["asks"]), depth)
        self.assertEqual(len(orderbook["bids"]), depth)
        return


class TestBitget(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bitget = Bitget()
        await self.bitget.sync_exchange_info()

    async def asyncTearDown(self):
        await self.bitget.close()

    async def test_get_ticker(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            ticker = await self.bitget.get_ticker(i)
            self.assertTrue(ticker)
        return

    async def test_get_tickers(self):
        spot = await self.bitget.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.bitget.get_tickers("perp")
        self.assertTrue(perp)

        future = await self.bitget.get_tickers("futures")
        self.assertTrue(future)

        tickers = await self.bitget.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_last_price(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            last_price = await self.bitget.get_last_price(i)
            self.assertTrue(last_price)
        return

    async def test_get_index_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            mark_price = await self.bitget.get_index_price(i)
            self.assertTrue(mark_price)
        return

    async def test_get_mark_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            mark_price = await self.bitget.get_mark_price(i)
            self.assertTrue(mark_price)
        return

    async def test_get_orderbook(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]
        depth = 75

        for i in [spot, perp, future]:
            orderbook = await self.bitget.get_orderbook(i, depth)
            self.assertEqual(len(orderbook["asks"]), depth)
            self.assertEqual(len(orderbook["bids"]), depth)

    async def test_get_current_funding_rate(self):
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [perp, futures]:
            funding_rate = await self.bitget.get_current_funding_rate(i)
            self.assertTrue(funding_rate, f"{i} {funding_rate}")
        return

    async def test_get_history_funding_rate_with_num(self):
        perp = "BTC/USDT:USDT-PERP"
        num = 120

        funding_rate = await self.bitget.get_history_funding_rate(perp, num=num)
        self.assertEqual(len(funding_rate), num, msg=f"{perp} {len(funding_rate)}")
        return

    async def test_get_history_funding_rate_with_timestamp(self):
        perp = "BTC/USDT:USDT-PERP"

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1

        funding_rate = await self.bitget.get_history_funding_rate(perp, start=start, end=end)
        self.assertEqual(len(funding_rate), target_num)
        return True


class TestBybit(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bybit_public = Bybit()
        await self.bybit_public.sync_exchange_info()

    async def asyncTearDown(self):
        await self.bybit_public.close()

    async def test_get_tickers(self):
        spot = await self.bybit_public.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.bybit_public.get_tickers("perp")
        self.assertTrue(perp)

        future = await self.bybit_public.get_tickers("futures")
        self.assertTrue(future)

        tickers = await self.bybit_public.get_tickers()
        self.assertTrue(tickers)

        return

    async def test_get_funding_rate(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        num = 33
        funding_rate = await self.bybit_public.get_history_funding_rate(instrument_id=instrument_id, num=num)
        self.assertEqual(len(funding_rate), num)

        start = int(dt(2024, 3, 1).timestamp() * 1000)
        end = int(dt(2024, 3, 3).timestamp() * 1000)
        funding_rate = await self.bybit_public.get_history_funding_rate(
            instrument_id=instrument_id, start=start, end=end
        )
        self.assertEqual(len(funding_rate), 7)
        return

    async def test_get_open_interest(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        interval = "5m"

        oi = await self.bybit_public.get_open_interest(instrument_id, interval)
        self.assertTrue(oi)
        return

    async def test_get_orderbook(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        depth = 100
        orderbook = await self.bybit_public.get_orderbook(instrument_id, depth)
        self.assertTrue(orderbook)
        return

    async def test_get_last_price(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        last_price = await self.bybit_public.get_last_price(instrument_id)
        self.assertTrue(last_price)
        return

    async def test_get_index_price(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        index_price = await self.bybit_public.get_index_price(instrument_id)
        self.assertTrue(index_price)
        return

    async def test_get_mark_price(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        mark_price = await self.bybit_public.get_mark_price(instrument_id)
        self.assertTrue(mark_price)
        return

    # Private test

    async def test_get_account_balance(self):
        balance = await self.bybit_private.get_account_balance()
        self.assertTrue(balance)
        return


class TestOutputStructure(IsolatedAsyncioTestCase):
    ticker_structure = {
        "timestamp": int,
        "instrument_id": str,
        "open_time": int,
        "close_time": int,
        "open": float,
        "high": float,
        "low": float,
        "last": float,
        "base_volume": float,
        "quote_volume": float,
        "price_change": float,
        "price_change_percent": float,
        "raw_data": dict,
    }

    async def asyncSetUp(self):
        self.okx_public = Okx()
        self.binace_public = Binance()
        self.kucoin_public = Kucoin()
        self.bitget_public = Bitget()
        self.bybit_public = Bybit()
        self.htx_public = Htx()
        self.exchange_list = [
            self.okx_public,
            self.binace_public,
            self.kucoin_public,
            self.bitget_public,
            self.bybit_public,
            self.htx_public,
        ]

        for i in self.exchange_list:
            await i.sync_exchange_info()

    async def asyncTearDown(self):
        for i in self.exchange_list:
            await i.close()

    async def test_ticker_output_structure(self):
        """
        Test if the output of get_ticker() has the correct structure
        1. all the keys in the output should be the same
        2. all the values should be the correct type
        3. no other keys should be present
        """
        for exchange in self.exchange_list:
            spot = "BTC/USDT:USDT"
            perp = "BTC/USDT:USDT-PERP"
            futures = list(filter(lambda x: exchange.exchange_info[x]["is_futures"], exchange.exchange_info))[0]

            for instrument_id in [spot, perp, futures]:
                ticker = await exchange.get_ticker(instrument_id)
                print(exchange.name, ticker)
                data = ticker[instrument_id]
                # check if all the keys are the same
                self.assertEqual(
                    set(data.keys()), set(self.ticker_structure.keys()), msg=f"{exchange.name} {instrument_id} {data}"
                )

                # check if all the values are the correct type
                for key, value in data.items():
                    if value:
                        self.assertIsInstance(
                            value, self.ticker_structure[key], msg=f"{exchange.name} {instrument_id} {key} {value}"
                        )
        return
