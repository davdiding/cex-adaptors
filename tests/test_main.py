# from datetime import datetime as dt
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


if __name__ == "__main__":
    import unittest

    unittest.main()
