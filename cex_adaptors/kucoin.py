import asyncio
import time

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

    async def close(self):
        await self.spot.close()
        await self.futures.close()

    async def sync_exchange_info(self):
        self.exchange_info = await self.get_exchange_info()

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
            num_batch = 30
            results = {}
            for i in range(0, len(ids), num_batch):
                tasks = []
                for instrument_id in ids[i : i + num_batch]:
                    _symbol = self.exchange_info[instrument_id]["raw_data"]["symbol"]
                    tasks.append(self.futures._get_symbol_detail(_symbol))
                raw_tickers = await asyncio.gather(*tasks)
                # rest for 5 sec for every 5 requests to avoid rate limit
                if i % 5 == 0:
                    time.sleep(3)
                parsed_tickers = self.parser.parse_derivative_tickers(raw_tickers, self.exchange_info)
                results.update(parsed_tickers)
            return results

        if market_type == "spot":
            return self.parser.parse_spot_tickers(await self.spot._get_tickers(), self.exchange_info)
        else:

            spot_tickers = self.parser.parse_spot_tickers(await self.spot._get_tickers(), self.exchange_info)
            derivative_tickers = await _get_derivative_tickers()
            tickers = {**spot_tickers, **derivative_tickers}

            if market_type:
                ids = list(query_dict(self.exchange_info, f"is_{market_type} == True").keys())
                return {k: v for k, v in tickers.items() if k in ids}
            else:
                return tickers

    async def get_ticker(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not found in {self.name} exchange info.")

        info = self.exchange_info[instrument_id]
        market_type = "spot" if info["is_spot"] else "derivative"
        _symbol = info["raw_data"]["symbol"]

        method_map = {
            "spot": self.spot._get_24hr_stats,
            "derivative": self.futures._get_symbol_detail,
        }

        return {instrument_id: self.parser.parse_ticker(await method_map[market_type](_symbol), info, market_type)}

    async def get_last_price(self, instrument_id: str) -> dict:
        ticker = await self.get_ticker(instrument_id)
        return {
            "timestamp": ticker["timestamp"],
            "instrument_id": instrument_id,
            "last_price": ticker["last"],
            "raw_data": ticker["raw_data"],
        }

    async def get_mark_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not found in {self.name} exchange info.")

        info = self.exchange_info[instrument_id]
        market_type = "spot" if info["is_spot"] else "derivative"
        _symbol = info["raw_data"]["symbol"]
        method_map = {
            "spot": self.spot._get_margin_mark_price,
            "derivative": self.futures._get_current_mark_price,
        }
        return self.parser.parse_mark_price(await method_map[market_type](_symbol), info, market_type)

    async def get_index_price(self, instrument_id: str):
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not found in {self.name} exchange info.")

        info = self.exchange_info[instrument_id]

        if info["is_spot"]:
            raise ValueError(
                f"{instrument_id} is a spot market. Index price is not available for spot markets in `{self.name}`."
            )

        market_type = "spot" if info["is_spot"] else "derivative"
        _symbol = info["raw_data"]["symbol"]

        return self.parser.parse_index_price(await self.futures._get_current_mark_price(_symbol), info, market_type)

    async def get_orderbook(self, instrument_id: str, depth: int = None):
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not found in {self.name} exchange info.")

        info = self.exchange_info[instrument_id]
        market_type = "spot" if info["is_spot"] else "derivative"
        _symbol = info["raw_data"]["symbol"]
        method_map = {
            "spot": self.spot._get_full_orderbook,
            "derivative": self.futures._get_full_orderbook,
        }

        results = self.parser.parse_orderbook(await method_map[market_type](_symbol), info, market_type)

        results["bids"] = sorted(results["bids"], key=lambda x: x["price"], reverse=True)
        results["asks"] = sorted(results["asks"], key=lambda x: x["price"])
        if depth:
            results["bids"] = results["bids"][:depth]
            results["asks"] = results["asks"][:depth]
        return results

    async def get_current_candlestick(self, instrument_id: str, interval: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not found in {self.name} exchange info.")

        info = self.exchange_info[instrument_id]
        _symbol = info["raw_data"]["symbol"]
        market_type = "spot" if info["is_spot"] else "derivative"
        _interval = self.parser.get_interval(interval, market_type)

        method_map = {
            "spot": self.spot._get_klines,
            "derivative": self.futures._get_klines,
        }

        params = {
            "symbol": _symbol,
            "granularity" if market_type == "derivative" else "type": _interval,
        }

        return {
            instrument_id: self.parser.parse_current_candlestick(
                await method_map[market_type](**params), info, market_type, interval
            )
        }

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

    async def get_current_funding_rate(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not found in {self.name} exchange info.")

        info = self.exchange_info[instrument_id]
        _symbol = info["raw_data"]["symbol"]

        return {
            instrument_id: self.parser.parse_current_funding_rate(
                await self.futures._get_current_funding_rate(_symbol), info
            )
        }

    async def get_history_funding_rate(
        self, instrument_id: str, start: int = None, end: int = None, num: int = None
    ) -> list:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not found in {self.name} exchange info.")

        info = self.exchange_info[instrument_id]
        _symbol = info["raw_data"]["symbol"]

        params = {"symbol": _symbol}

        results = []
        query_end = end if end else self.parser.get_timestamp()
        query_start = start if start else query_end - 10 * 365 * 24 * 60 * 60 * 1000
        implied_limit = 100
        if start and end:
            while True:
                params.update({"to": query_end, "_from": query_start})
                result = self.parser.parse_history_funding_rate(
                    await self.futures._get_public_funding_history(**params), info
                )
                results.extend(result)

                # exclude data with same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < implied_limit:
                    break

                query_end = min([v["timestamp"] for v in result])

                if min([v["timestamp"] for v in results]) < start:
                    break
                continue

            return sorted(
                [v for v in results if start <= v["timestamp"] <= end], key=lambda x: x["timestamp"], reverse=False
            )
        elif num:
            while True:
                params.update({"to": query_end, "_from": query_start})
                result = self.parser.parse_history_funding_rate(
                    await self.futures._get_public_funding_history(**params), info
                )
                results.extend(result)

                # exclude data with same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < implied_limit or len(results) >= num:
                    break

                query_end = min([v["timestamp"] for v in results])
                continue
            return sorted(results, key=lambda x: x["timestamp"], reverse=False)[-num:]

        else:
            raise ValueError("(start, end) or num must be provided")
