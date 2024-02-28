import tracemalloc
from typing import Literal, Optional

from .exchanges.binance import BinanceInverse, BinanceLinear, BinanceSpot
from .parsers.binance import BinanceParser
from .utils import sort_dict

tracemalloc.start()


class Binance(object):
    name = "binance"

    def __init__(self):
        self.spot = BinanceSpot()
        self.linear = BinanceLinear()
        self.inverse = BinanceInverse()
        self.parser = BinanceParser()

        self.exchange_info = {}

    async def close(self):
        await self.spot.close()
        await self.linear.close()
        await self.inverse.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: Optional[Literal["spot", "margin", "futures", "perp"]] = None):
        spot = self.parser.parse_exchange_info(
            await self.spot._get_exchange_info(), self.parser.spot_exchange_info_parser
        )
        linear = self.parser.parse_exchange_info(
            await self.linear._get_exchange_info(), self.parser.futures_exchange_info_parser("linear")
        )
        inverse = self.parser.parse_exchange_info(
            await self.inverse._get_exchange_info(), self.parser.futures_exchange_info_parser("inverse")
        )
        result = {**spot, **linear, **inverse}
        return result if not market_type else self.parser.query_dict(result, {f"is_{market_type}": True})

    async def get_ticker(self, instrument_id: str):
        _symbol = self.exchange_info[instrument_id]["raw_data"]["symbol"]
        info = self.exchange_info[instrument_id]
        market_type = self.parser.get_market_type(info)

        if market_type == "spot":
            return {instrument_id: self.parser.parse_ticker(await self.spot._get_ticker(_symbol), info)}
        elif market_type == "linear":
            return {instrument_id: self.parser.parse_ticker(await self.linear._get_ticker(_symbol), info)}
        elif market_type == "inverse":
            return {instrument_id: self.parser.parse_ticker(await self.inverse._get_ticker(_symbol), info)}

    async def get_tickers(self, market_type: Optional[Literal["spot", "margin", "futures", "perp"]] = None) -> dict:
        results = {}

        tickers = [(self.spot, "spot"), (self.linear, "linear"), (self.inverse, "inverse")]

        for exchange, _market_type in tickers:
            parsed_tickers = self.parser.parse_tickers(await exchange._get_tickers(), _market_type, self.exchange_info)
            results.update(parsed_tickers)

        if market_type:
            ids = list(self.parser.query_dict(self.exchange_info, {f"is_{market_type}": True}).keys())
            return self.parser.query_dict_by_keys(results, ids)
        else:
            return results

    async def get_klines(self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = 500):
        _symbol = self.exchange_info[instrument_id]["raw_data"]["symbol"]
        _interval = self.parser.get_interval(interval)
        limit = 1000
        market_type = self.parser.get_market_type(self.exchange_info[instrument_id])

        params = {
            "symbol": _symbol,
            "interval": _interval,
            "limit": limit,
        }

        method_map = {
            "spot": self.spot._get_klines,
            "linear": self.linear._get_klines,
            "inverse": self.inverse._get_klines,
        }

        query_end = None

        results = {}
        if start and end:
            query_end = end
            while True:
                params["endTime"] = query_end
                klines = self.parser.parse_klines(await method_map[market_type](**params), market_type)
                if not klines:
                    break

                results.update(klines)
                query_end = sorted(list(klines.keys()))[0]
                if len(klines) < limit or query_end <= start:
                    break
                continue
            return sort_dict({k: v for k, v in results.items() if end >= k >= start}, ascending=True)

        elif num:
            while True:
                params.update({"endTime": query_end} if query_end else {})
                klines = self.parser.parse_klines(
                    await method_map[market_type](**params),
                    market_type,
                )
                results.update(klines)
                if len(klines) < limit or len(results) >= num:
                    break
                query_end = sorted(list(klines.keys()))[0]
                continue

            return sort_dict(results, ascending=True, num=num)
