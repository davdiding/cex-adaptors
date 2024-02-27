from .exchanges.htx import HtxFutures, HtxUnified
from .parsers.htx import HtxParser
from .utils import query_dict, sort_dict


class Htx(object):
    name = "htx"

    def __init__(self):
        self.spot = HtxUnified()
        self.futures = HtxFutures()
        self.parser = HtxParser()
        self.exchange_info = {}

    async def close(self):
        await self.spot.close()
        await self.futures.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self, market_type: str = None):
        spot = self.parser.parse_exchange_info(
            await self.spot._get_exchange_info(), self.parser.spot_exchange_info_parser
        )
        linear = self.parser.parse_exchange_info(
            await self.futures._get_linear_contract_info(), self.parser.linear_exchange_info_parser
        )
        inverse_futures = self.parser.parse_exchange_info(
            await self.futures._get_inverse_futures_info(), self.parser.inverse_futures_exchange_info_parser
        )
        inverse_perp = self.parser.parse_exchange_info(
            await self.futures._get_inverse_perp_info(), self.parser.inverse_perp_exchange_info_parser
        )

        return {**spot, **linear, **inverse_futures, **inverse_perp}

    async def get_tickers(self, market_type: str = None):
        # get_all tickers then filter by market_type
        if market_type:
            if market_type == "spot":
                return self.parser.parse_tickers(await self.spot._get_tickers(), self.exchange_info, "spot")
            else:
                linear = self.parser.parse_tickers(
                    await self.futures._get_linear_contract_tickers(), self.exchange_info, "linear"
                )
                inverse_perp = self.parser.parse_tickers(
                    await self.futures._get_inverse_perp_tickers(), self.exchange_info, "inverse_perp"
                )

                inverse_futures = self.parser.parse_tickers(
                    await self.futures._get_inverse_futures_tickers(), self.exchange_info, "inverse_futures"
                )
                results = {**linear, **inverse_perp, **inverse_futures}

                instrument_id = list(query_dict(self.exchange_info, f"is_{market_type} == True").keys())
                return {k: v for k, v in results.items() if k in instrument_id}
        else:
            spot = self.parser.parse_tickers(await self.spot._get_tickers(), self.exchange_info, "spot")

            linear = self.parser.parse_tickers(
                await self.futures._get_linear_contract_tickers(), self.exchange_info, "linear"
            )
            inverse_perp = self.parser.parse_tickers(
                await self.futures._get_inverse_perp_tickers(), self.exchange_info, "inverse_perp"
            )
            inverse_futures = self.parser.parse_tickers(
                await self.futures._get_inverse_futures_tickers(), self.exchange_info, "inverse_futures"
            )
            return {**spot, **linear, **inverse_perp, **inverse_futures}

    async def get_klines(self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None):
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not in exchange_info")

        info = self.exchange_info[instrument_id]
        market_type = self.parser.get_market_type(info)
        _interval = self.parser.get_interval(interval, market_type)

        symbol_map = {
            "spot": "sc",
            "linear": "contract_code",
            "inverse_futures": "symbol",
            "inverse_perp": "contract_code",
        }

        method_map = {
            "spot": self.spot._get_klines,
            "linear": self.futures._get_linear_contract_klines,
            "inverse_futures": self.futures._get_inverse_futures_klines,
            "inverse_perp": self.futures._get_inverse_perp_klines,
        }

        limit_map = {
            "spot": 2000,
            "linear": 2000,
            "inverse_futures": 2000,
            "inverse_perp": 2000,
        }

        params = {
            "symbol": info["raw_data"][symbol_map[market_type]]
            if market_type != "inverse_futures"
            else self.parser.parse_inverse_futures_symbol(info["raw_data"]),
            "period": _interval,
            "limit": limit_map[market_type],
        }
        results = {}
        query_end = None
        if start and end and market_type != "spot":
            query_end = end
            while True:
                params["end"] = str(query_end)[:10]
                result = self.parser.parse_klines(await method_map[market_type](**params), market_type, info)
                results.update(result)

                temp_end = str(sorted(list(result.keys()))[0])[:10]
                if len(result) < limit_map[market_type] or temp_end == query_end:
                    break

                query_end = temp_end  # get the earliest timestamp in 10 digits
                if query_end <= start:
                    break
            return {k: v for k, v in results.items() if start <= int(k) <= end}

        elif num:
            while True:
                params.update({"end": query_end} if query_end else {})
                result = self.parser.parse_klines(await method_map[market_type](**params), market_type, info)
                results.update(result)

                temp_end = str(sorted(list(result.keys()))[0])[:10]
                if len(result) < limit_map[market_type] or temp_end == query_end:
                    break

                query_end = temp_end  # get the earliest timestamp in 10 digits
                if len(results) >= num:
                    break
            return sort_dict(results, ascending=True, num=num)

        else:
            raise ValueError("(start, end) or num must be provided")
