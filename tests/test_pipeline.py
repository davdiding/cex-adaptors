from unittest import IsolatedAsyncioTestCase

from cex_services.bybit import Bybit


class TestBybit(IsolatedAsyncioTestCase):
    async def test_get_exchange_info(self):
        bybit = await Bybit.create()
        response = await bybit.get_exchange_info()
        self.assertTrue(response)
        await bybit.close()
        return
