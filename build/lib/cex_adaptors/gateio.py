from .exchanges.gateio import GateioClient
from .parsers.gateio import GateioParser
from .utils import sort_dict


class Gateio(object):
    name = "gateio"

    PERP_SETTLE = ["btc", "usdt", "usd"]

    def __init__(self):
        self.exchange = GateioClient()
        self.parser = GateioParser()
        self.exchange_info = {}

    async def close(self):
        await self.exchange.close()

    @classmethod
    async def create(cls):
        instance = cls()
        instance.exchange_info = await instance.get_exchange_info()
        return instance

    async def get_exchange_info(self):
        spot = self.parser.parse_exchange_info(
            await self.exchange._get_currency_pairs(), self.parser.spot_exchange_info_parser
        )

        perps = {}
        for settle in self.PERP_SETTLE:
            perp = self.parser.parse_exchange_info(
                await self.exchange._get_perp_info(settle),
                self.parser.perp_exchange_info_parser,
                settle=settle.upper(),
            )
            perps.update(perp)

        settle = "usdt"
        futures = self.parser.parse_exchange_info(
            await self.exchange._get_futures_info(settle),
            self.parser.futures_exchange_info_parser,
            settle=settle.upper(),
        )

        return {**spot, **perps, **futures}

    async def get_tickers(self, market_type: str = None) -> dict:
        if market_type == "spot":
            return self.parser.parse_tickers(await self.exchange._get_spot_tickers(), self.exchange_info, "spot")
        elif market_type == "futures":
            return self.parser.parse_tickers(
                await self.exchange._get_futures_tickers(settle="usdt"), self.exchange_info, "futures"
            )
        elif market_type == "perp":
            perps = {}
            for settle in self.PERP_SETTLE:
                perp = self.parser.parse_tickers(
                    await self.exchange._get_perp_tickers(settle), self.exchange_info, "perp"
                )
                perps.update(perp)
            return perps
        else:
            spot = self.parser.parse_tickers(await self.exchange._get_spot_tickers(), self.exchange_info, "spot")
            futures = self.parser.parse_tickers(
                await self.exchange._get_futures_tickers(settle="usdt"), self.exchange_info, "futures"
            )
            perps = {}
            for settle in self.PERP_SETTLE:
                perp = self.parser.parse_tickers(
                    await self.exchange._get_perp_tickers(settle), self.exchange_info, "perp"
                )
                perps.update(perp)
            return {**spot, **futures, **perps}

    async def get_klines(
        self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None
    ) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange_info")

        info = self.exchange_info[instrument_id]
        market_type = self.parser.get_market_type(info)
        _interval = self.parser.get_interval(interval)
        limit_map = {"spot": 1000, "futures": 2000, "perp": 2000}

        symbol_map = {
            "spot": "id",
            "futures": "name",
            "perp": "name",
        }

        method_map = {
            "spot": self.exchange._get_spot_klines,
            "futures": self.exchange._get_futures_klines,
            "perp": self.exchange._get_perp_klines,
        }

        params = {
            "symbol": info["raw_data"][symbol_map[market_type]],
            "interval": _interval,
            "limit": limit_map[market_type],
        }

        # handle special market type params
        if market_type in ["futures", "perp"]:
            params["settle"] = info["settle"].lower()

        results = {}
        query_end = None
        if start and end:
            query_end = str(end)[:10]
            while True:
                params.update({"end": query_end})
                result = self.parser.parse_klines(await method_map[market_type](**params), market_type, info)
                results.update(result)

                if len(result) < limit_map[market_type]:
                    break

                query_end = sorted(list(result.keys()))[0]

                if query_end < start:
                    break

                query_end = str(query_end)[:10]
                continue

            return {k: v for k, v in results.items() if start <= k <= end}

        elif num:
            while True:
                params.update({"end": query_end} if query_end else {})
                result = self.parser.parse_klines(await method_map[market_type](**params), market_type, info)
                results.update(result)

                if len(result) < limit_map[market_type] or len(results) >= num:
                    break
                query_end = str(sorted(list(result.keys()))[0])[:10]

            return sort_dict(results, ascending=True, num=num)

        else:
            raise ValueError("(start, end) or num must be provided")
