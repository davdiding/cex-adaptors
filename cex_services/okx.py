import tracemalloc

from .exchanges.okx import OkxUnified
from .parsers.okx import OkxParser

tracemalloc.start()


class Okx(OkxUnified):
    name = "okx"
    market_type_map = {"spot": "SPOT", "margin": "MARGIN", "futures": "FUTURES", "perp": "SWAP"}
    _market_type_map = {"SPOT": "spot", "MARGIN": "margin", "FUTURES": "futures", "SWAP": "perp"}

    def __init__(self):
        super().__init__()

        self.parser = OkxParser()
        self.exchange_info = {}

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        if market_type:
            parser = (
                self.parser.spot_margin_exchange_info_parser
                if market_type in ["spot", "margin"]
                else self.parser.futures_perp_exchange_info_parser
            )
            return self.parser.parse_exchange_info(
                await self._get_exchange_info(self.market_type_map[market_type]), parser
            )

        else:
            spot = self.parser.parse_exchange_info(
                await self._get_exchange_info("SPOT"), self.parser.spot_margin_exchange_info_parser
            )
            margin = self.parser.parse_exchange_info(
                await self._get_exchange_info("MARGIN"), self.parser.spot_margin_exchange_info_parser
            )
            futures = self.parser.parse_exchange_info(
                await self._get_exchange_info("FUTURES"), self.parser.futures_perp_exchange_info_parser
            )
            perp = self.parser.parse_exchange_info(
                await self._get_exchange_info("SWAP"), self.parser.futures_perp_exchange_info_parser
            )
            exchange_info = {**self.parser.combine_spot_margin_exchange_info(spot, margin), **futures, **perp}
        return exchange_info

    async def get_tickers(self, market_type: str = None) -> list:

        if market_type == "spot":
            return self.parser.parse_tickers(await self._get_tickers("SPOT"), "spot", self.exchange_info)
        elif market_type == "futures":
            return self.parser.parse_tickers(await self._get_tickers("FUTURES"), "futures", self.exchange_info)
        elif market_type == "perp":
            return self.parser.parse_tickers(await self._get_tickers("SWAP"), "perp", self.exchange_info)
        else:
            results = {}
            for market_type in ["spot", "futures", "perp"]:
                _market_type = self.market_type_map[market_type]
                parsed_tickers = self.parser.parse_tickers(
                    await self._get_tickers(_market_type), market_type, self.exchange_info
                )
                results.update(parsed_tickers)

            return results

    async def get_ticker(self, instrument_id: str):
        _instrument_id = self.exchange_info[instrument_id]["raw_data"]["instId"]
        market_type = self._market_type_map[self.exchange_info[instrument_id]["raw_data"]["instType"]]
        return {instrument_id: self.parser.parse_ticker(await self._get_ticker(_instrument_id), market_type)}

    async def get_klines(self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None):
        info = self.exchange_info[instrument_id]
        market_type = self._market_type_map[info["raw_data"]["instType"]]
        _instrument_id = info["raw_data"]["instId"]
        _interval = self.parser.get_interval(interval)
        limit = 300

        params = {"instId": _instrument_id, "bar": _interval, "limit": limit}

        results = {}
        if start and end:
            query_end = end
            while True:
                params.update({"after": query_end})
                datas = self.parser.parse_klines(
                    await self._get_klines(**params),
                    market_type,
                )
                results.update(datas)

                if not datas or len(datas) < limit:
                    break
                query_end = sorted(datas.keys())[0]
                if query_end < start:
                    break
            results = {k: v for k, v in results.items() if start <= k <= end}

        elif start:
            query_end = end
            while True:
                params.update({"after": query_end} if query_end else {})
                datas = self.parser.parse_klines(
                    await self._get_klines(**params),
                    market_type,
                )
                results.update(datas)
                if not datas or len(datas) < limit:
                    break
                query_end = sorted(datas.keys())[0]
                if query_end < start:
                    break
            results = {k: v for k, v in results.items() if k >= start}
        elif end and num:
            query_end = end
            query_num = min(num, limit)
            while True:
                params.update({"after": query_end, "limit": query_num})
                datas = self.parser.parse_klines(
                    await self._get_klines(**params),
                    market_type,
                )
                results.update(datas)
                if not datas or len(datas) < limit:
                    break
                query_num = min(num - len(results), limit)
                query_end = sorted(datas.keys())[0]
            results = {k: v for k, v in results.items() if k <= end}
        elif num:
            query_end = end
            query_num = min(num, limit)
            while True:
                params.update({"after": query_end, "limit": query_num} if query_end else {"limit": query_num})
                datas = self.parser.parse_klines(
                    await self._get_klines(**params),
                    market_type,
                )
                results.update(datas)
                if not datas or len(datas) < limit:
                    break
                query_num = min(num - len(results), limit)
                query_end = sorted(datas.keys())[0]

            results = dict(sorted(results.items(), key=lambda x: x[0])[-num:])
        else:
            raise Exception("invalid params")

        return results
