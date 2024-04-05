import tracemalloc
from typing import Literal, Optional

from .exchanges.binance import BinanceInverse, BinanceLinear, BinanceSpot
from .parsers.binance import BinanceParser
from .utils import sort_dict

tracemalloc.start()


class Binance(object):
    name = "binance"

    def __init__(self, api_key: str = None, api_secret: str = None):
        self.spot = BinanceSpot(api_key=api_key, api_secret=api_secret)
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

    async def sync_exchange_info(self) -> None:
        self.exchange_info = await self.get_exchange_info()

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

    async def get_history_funding_rate(
        self, instrument_id: str, start: int = None, end: int = None, num: int = 30
    ) -> list:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange info")

        info = self.exchange_info[instrument_id]
        market_type = self.parser.get_market_type(info)
        limit = 1000
        symbol = info["raw_data"]["symbol"]

        method_map = {
            "linear": self.linear._get_funding_rate_history,
            "inverse": self.inverse._get_funding_rate_history,
        }

        params = {
            "symbol": symbol,
            "limit": limit,
        }

        results = []
        query_end = None
        if start and end:
            query_start = start - 1
            query_end = end + 1
            while True:
                params.update({"startTime": query_start, "endTime": query_end})
                result = self.parser.parse_history_funding_rate(await method_map[market_type](**params), info)
                results.extend(result)
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < limit:
                    break

                query_start = max([v["timestamp"] for v in result]) - 1

                if query_start < end:
                    break

                continue
            return sorted(
                [v for v in results if end >= v["timestamp"] >= start], key=lambda x: x["timestamp"], reverse=False
            )

        elif num:
            while True:
                params.update({"endTime": query_end} if query_end else {})
                result = self.parser.parse_history_funding_rate(await method_map[market_type](**params), info)
                results.extend(result)

                # exclude the datas with same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if len(result) < limit or len(results) >= num:
                    break
                query_end = min([v["timestamp"] for v in result]) + 1
                continue
            return sorted(results, key=lambda x: x["timestamp"], reverse=False)[-num:]
        else:
            raise ValueError("(start, end) or num must be provided")

    # Private function
    async def get_spot_account_info(self) -> dict:
        return self.parser.parse_spot_account_info(await self.spot._get_account_info())

    async def get_margin_account_info(self) -> dict:
        return self.parser.parse_margin_account_info(await self.spot._get_margin_account_info())

    async def get_margin_balance(self, currency: str = None) -> dict:
        return self.parser.parse_margin_balance(await self.spot._get_margin_account_info(), currency=currency)

    async def get_margin_account_value(self, in_currency: str = None):
        info = await self.get_margin_account_info()

        net_btc_value = self.parser.parse_str(info["raw_data"]["totalNetAssetOfBtc"], float)

        instrument_id = "BTC/USDT:USDT"
        ticker = await self.get_ticker(instrument_id)
        btc_price = ticker[instrument_id]["last_price"]

        if in_currency:
            instrument_id = f"{in_currency}/USDT:USDT"
            ccy_ticker = await self.get_ticker(instrument_id)
            ccy_price = ccy_ticker[instrument_id]["last_price"]

            return net_btc_value * btc_price / ccy_price

        else:
            return net_btc_value * btc_price
