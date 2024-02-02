from .exchanges.okx import OkxUnified
from .parsers.okx import OkxParser


class Okx(OkxParser, OkxUnified):
    name = "okx"
    market_type_map = {"spot": "SPOT", "margin": "MARGIN", "futures": "FUTURES", "perp": "SWAP"}
    _market_type_map = {"SPOT": "spot", "MARGIN": "margin", "FUTURES": "futures", "SWAP": "perp"}

    def __init__(self):
        super().__init__()
        self.exchange_info = {}

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        if market_type:
            parser = (
                self.spot_margin_exchange_info_parser
                if market_type in ["spot", "margin"]
                else self.futures_perp_exchange_info_parser
            )
            return self.parse_exchange_info(await self._get_exchange_info(self.market_type_map[market_type]), parser)

        else:
            spot = self.parse_exchange_info(
                await self._get_exchange_info("SPOT"), self.spot_margin_exchange_info_parser
            )
            margin = self.parse_exchange_info(
                await self._get_exchange_info("MARGIN"), self.spot_margin_exchange_info_parser
            )
            futures = self.parse_exchange_info(
                await self._get_exchange_info("FUTURES"), self.futures_perp_exchange_info_parser
            )
            perp = self.parse_exchange_info(
                await self._get_exchange_info("SWAP"), self.futures_perp_exchange_info_parser
            )
            exchange_info = {**self.combine_spot_margin_exchange_info(spot, margin), **futures, **perp}
        return exchange_info

    async def get_tickers(self, market_type: str = None) -> list:

        results = {}

        tickers = [("SPOT", "spot"), ("FUTURES", "futures"), ("SWAP", "perp")]

        for _market_type, market_type in tickers:
            if market_type != market_type:
                continue
            parsed_tickers = self.parse_tickers(await self._get_tickers(_market_type), market_type)
            id_map = self.get_id_symbol_map(self.exchange_info, market_type, "instId")

            for ticker in parsed_tickers:
                symbol = ticker["symbol"]
                if symbol not in id_map:
                    print(symbol)
                    continue
                results[id_map[symbol]] = ticker
        return results

    async def get_ticker(self, id: str):
        _id = self.exchange_info[id]["raw_data"]["instId"]
        market_type = self._market_type_map[self.exchange_info[id]["raw_data"]["instType"]]
        return {id: self.parse_ticker(await self._get_ticker(_id), market_type)}

    async def get_klines(self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None):
        info = self.exchange_info[instrument_id]
        market_type = self._market_type_map[info["raw_data"]["instType"]]
        _instrument_id = info["raw_data"]["instId"]
        _interval = self.get_interval(interval)
        limit = 300

        results = {}
        if start and end:
            query_end = end
            query_num = limit
            while True:
                datas = self.parse_klines(
                    await self._get_klines(instId=_instrument_id, after=query_end, bar=_interval, limit=query_num),
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
            query_num = limit
            while True:
                datas = self.parse_klines(
                    await self._get_klines(instId=_instrument_id, bar=_interval, limit=query_num)
                    if not query_end
                    else await self._get_klines(instId=_instrument_id, after=query_end, bar=_interval, limit=query_num),
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
                datas = self.parse_klines(
                    await self._get_klines(instId=_instrument_id, after=query_end, bar=_interval, limit=query_num),
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
                datas = self.parse_klines(
                    await self._get_klines(instId=_instrument_id, bar=_interval, limit=query_num)
                    if not query_end
                    else await self._get_klines(instId=_instrument_id, after=query_end, bar=_interval, limit=query_num),
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
