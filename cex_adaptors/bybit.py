from typing import Literal, Optional

from .exchanges.bybit import BybitUnified
from .parsers.bybit import BybitParser
from .utils import sort_dict


class Bybit(BybitUnified):
    name = "bybit"

    def __init__(self):
        super().__init__()
        self.parser = BybitParser()
        self.exchange_info = {}

    async def sync_exchange_info(self):
        self.exchange_info = await self.get_exchange_info()

    async def get_exchange_info(self, market_type: str = None):
        spot = self.parser.parse_exchange_info(
            await self._get_exchange_info("spot"), self.parser.spot_exchange_info_parser
        )
        linear = self.parser.parse_exchange_info(
            await self._get_exchange_info("linear"), self.parser.perp_futures_exchange_info_parser
        )
        inverse = self.parser.parse_exchange_info(
            await self._get_exchange_info("inverse"), self.parser.perp_futures_exchange_info_parser
        )

        return {**spot, **linear, **inverse}

    async def get_tickers(self, market_type: Optional[Literal["spot", "margin", "futures", "perp"]] = None):

        results = {}

        tickers = ["spot", "linear", "inverse"]

        for _market_type in tickers:
            parsed_tickers = self.parser.parse_tickers(await self._get_tickers(_market_type), _market_type)
            id_map = self.parser.get_id_symbol_map(self.exchange_info, _market_type)

            for ticker in parsed_tickers:
                symbol = ticker["symbol"]
                if symbol not in id_map:
                    print(symbol)
                    continue
                id = id_map[symbol]

                results[id] = ticker

        if market_type:
            ids = list(self.parser.query_dict(self.exchange_info, {f"is_{market_type}": True}).keys())
            return self.parser.query_dict_by_keys(results, ids)
        else:
            return results

    async def get_klines(self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = 30):
        _category = self.parser.get_category(self.exchange_info[instrument_id])
        _symbol = self.exchange_info[instrument_id]["raw_data"]["symbol"]
        _interval = self.parser.get_interval(interval)
        limit = 1000

        params = {"symbol": _symbol, "interval": _interval, "limit": limit, "category": _category}

        results = {}
        query_end = None
        if start and end:
            query_end = end
            while True:
                params["end"] = query_end
                klines = self.parser.parse_klines(await self._get_klines(**params))
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
                params.update({"end": query_end} if query_end else {})
                klines = self.parser.parse_klines(await self._get_klines(**params))
                results.update(klines)

                if len(klines) < limit or len(results) >= num:
                    break
                query_end = sorted(list(klines.keys()))[0]
                continue

            return sort_dict(results, ascending=True, num=num)
        else:
            raise ValueError("(start, end) or num must be provided")

    async def get_history_funding_rate(self, instrument_id: str, start: int = None, end: int = None, num: int = 30):
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} is not supported")

        info = self.exchange_info[instrument_id]
        _category = self.parser.get_category(info)
        _symbol = info["raw_data"]["symbol"]
        limit = 200

        params = {"symbol": _symbol, "limit": limit, "category": _category}

        results = []
        query_end = None
        if start and end:
            query_end = end + 1
            while True:
                params["endTime"] = query_end
                result = self.parser.parse_funding_rate(await self._get_funding_rate(**params), info)
                results.extend(result)

                # exclude data with same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < limit:
                    break

                query_end = min([v["timestamp"] for v in result])

                if query_end <= start:
                    break
                continue
            return sorted(
                [v for v in results if end >= v["timestamp"] >= start], key=lambda x: x["timestamp"], reverse=False
            )

        elif num:
            while True:
                params.update({"endTime": query_end} if query_end else {})
                result = self.parser.parse_funding_rate(await self._get_funding_rate(**params), info)

                results.extend(result)
                # exclude data with same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < limit or len(results) >= num:
                    break

                query_end = min([v["timestamp"] for v in result])
                continue
            return sorted(results, key=lambda x: x["timestamp"], reverse=False)[-num:]
        else:
            raise ValueError("(start, end) or num must be provided")

    async def get_open_interest(self, instrument_id: str, interval: str = "5m"):
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange info")

        info = self.exchange_info[instrument_id]
        _category = self.parser.get_category(info)
        _symbol = info["raw_data"]["symbol"]
        _interval = self.parser.get_open_interest_interval(interval)
        limit = 1
        params = {"category": _category, "symbol": _symbol, "interval": _interval, "limit": limit}

        return self.parser.parse_open_interest(await self._get_open_interest(**params), info)

    async def get_orderbook(self, instrument_id: str, depth: int = 100):
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange info")

        order_book_depth_map = {
            "spot": 200,
            "linear": 500,
            "inverse": 500,
        }

        info = self.exchange_info[instrument_id]
        _category = self.parser.get_category(info)
        _symbol = info["raw_data"]["symbol"]
        _depth = min(depth, order_book_depth_map[_category])

        params = {"category": _category, "symbol": _symbol, "limit": _depth}
        return self.parser.parse_orderbook(await self._get_orderbook(**params), info)

    async def get_last_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange info")

        info = self.exchange_info[instrument_id]
        _category = self.parser.get_category(info)
        _symbol = info["raw_data"]["symbol"]

        return self.parser.parse_last_price(await self._get_ticker(symbol=_symbol, category=_category), instrument_id)

    async def get_index_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange info")

        info = self.exchange_info[instrument_id]
        _category = self.parser.get_category(info)
        _symbol = info["raw_data"]["symbol"]

        return self.parser.parse_index_price(await self._get_ticker(symbol=_symbol, category=_category), instrument_id)

    async def get_mark_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange info")

        info = self.exchange_info[instrument_id]
        _category = self.parser.get_category(info)
        _symbol = info["raw_data"]["symbol"]

        return self.parser.parse_mark_price(await self._get_ticker(symbol=_symbol, category=_category), instrument_id)
