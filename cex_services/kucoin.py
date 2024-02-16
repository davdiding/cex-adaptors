import asyncio

from .exchanges.kucoin import KucoinFutures, KucoinSpot
from .parsers.kucoin import KucoinParser
from .utils import query_dict, sort_dict


class Kucoin(object):
    name = "kucoin"

    def __init__(self):
        self.spot = KucoinSpot()
        self.futures = KucoinFutures()
        self.parser = KucoinParser()

        self.exchange_info = {}

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def close(self):
        await self.spot.close()
        await self.futures.close()

    async def get_exchange_info(self) -> dict:
        spot = self.parser.parse_exchange_info(
            await self.spot._get_symbol_list(), self.parser.spot_exchange_info_parser
        )
        futures = self.parser.parse_exchange_info(
            await self.futures._get_symbol_list(), self.parser.futures_exchange_info_parser
        )

        return {**spot, **futures}

    async def get_tickers(self, market_type: str = None) -> dict:
        async def _get_derivative_tickers():
            ids = list(query_dict(self.exchange_info, "is_futures == True or is_perp == True").keys())
            num_batch = 20
            results = {}
            for i in range(0, len(ids), num_batch):
                tasks = []
                for instrument_id in ids[i : i + num_batch]:
                    _symbol = self.exchange_info[instrument_id]["raw_data"]["symbol"]
                    tasks.append(self.futures._get_contract_info(_symbol))
                raw_tickers = await asyncio.gather(*tasks)
                parsed_tickers = self.parser.parse_derivative_tickers(raw_tickers, self.exchange_info)
                results.update(parsed_tickers)
            return results

        if market_type == "spot":
            return self.parser.parse_spot_tickers(await self.spot._get_tickers(), self.exchange_info)

        elif market_type == "futures":
            return await _get_derivative_tickers()

        else:
            spot_tickers = self.parser.parse_spot_tickers(await self.spot._get_tickers(), self.exchange_info)
            derivative_tickers = await _get_derivative_tickers()
            return {**spot_tickers, **derivative_tickers}

    async def get_klines(self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None):
        info = self.exchange_info[instrument_id]
        market_type = "spot" if info["is_spot"] else "derivative"
        _symbol = info["raw_data"]["symbol"]
        _interval = self.parser.get_interval(interval, market_type)
        method_map = {
            "spot": self.spot._get_klines,
            "derivative": self.futures._get_klines,
        }
        limit_map = {
            "spot": 100,
            "derivative": 200,
        }

        params = {
            "symbol": _symbol,
            "granularity" if market_type == "derivative" else "type": _interval,
        }

        results = {}
        query_end = None
        if start and end:
            query_end = self.parser.parse_kucoin_timestamp(end, market_type)
            while True:
                params.update({"end": query_end})
                klines = self.parser.parse_klines(await method_map[market_type](**params), info, market_type)

                results.update(klines)
                if len(klines) < limit_map[market_type]:
                    break

                query_end = sorted(list(results.keys()))[0]
                if query_end < start:
                    break

                query_end = self.parser.parse_kucoin_timestamp(sorted(list(results.keys()))[0], market_type)
                continue
            return {k: v for k, v in results.items() if start <= k <= end}

        elif num:
            while True:
                params.update({"end": query_end} if query_end else {})
                klines = self.parser.parse_klines(await method_map[market_type](**params), info, market_type)

                results.update(klines)
                if len(results) >= num or len(klines) < limit_map[market_type]:
                    break

                query_end = (
                    int(sorted(list(results.keys()))[0] / 1000)
                    if market_type == "spot"
                    else sorted(list(results.keys()))[0]
                )
                continue
            return sort_dict(results, True, num)

        else:
            raise ValueError("Invalid parameters. (start, end) or (end, num) or (num) must be provided.")
