from .exchanges.gateio import GateioUnified
from .parsers.gateio import GateioParser


class Gateio(GateioUnified):
    name = "gateio"

    PERP_SETTLE = ["btc", "usdt", "usd"]

    def __init__(self):
        super().__init__()
        self.parser = GateioParser()
        self.exchange_info = {}

    async def sync_exchange_info(self):
        self.exchange_info = await self.get_exchange_info()

    async def get_exchange_info(self):
        spot = self.parser.parse_exchange_info(await self._get_currency_pairs(), self.parser.spot_exchange_info_parser)

        perps = {}
        for settle in self.PERP_SETTLE:
            perp = self.parser.parse_exchange_info(
                await self._get_perp_info(settle),
                self.parser.perp_exchange_info_parser,
                settle=settle.upper(),
            )
            perps.update(perp)

        settle = "usdt"
        futures = self.parser.parse_exchange_info(
            await self._get_futures_info(settle),
            self.parser.futures_exchange_info_parser,
            settle=settle.upper(),
        )

        return {**spot, **perps, **futures}

    async def get_tickers(self, market_type: str = None) -> dict:
        if market_type == "spot":
            return self.parser.parse_tickers(await self._get_spot_tickers(), self.exchange_info, "spot")
        elif market_type == "futures":
            return self.parser.parse_tickers(
                await self._get_futures_tickers(settle="usdt"), self.exchange_info, "futures"
            )
        elif market_type == "perp":
            perps = {}
            for settle in self.PERP_SETTLE:
                perp = self.parser.parse_tickers(await self._get_perp_tickers(settle), self.exchange_info, "perp")
                perps.update(perp)
            return perps
        else:
            spot = self.parser.parse_tickers(await self._get_spot_tickers(), self.exchange_info, "spot")
            futures = self.parser.parse_tickers(
                await self._get_futures_tickers(settle="usdt"), self.exchange_info, "futures"
            )
            perps = {}
            for settle in self.PERP_SETTLE:
                perp = self.parser.parse_tickers(await self._get_perp_tickers(settle), self.exchange_info, "perp")
                perps.update(perp)
            return {**spot, **futures, **perps}

    async def get_ticker(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in {self.name} exchange info")
        info = self.exchange_info[instrument_id]
        market_type = self.parser.get_market_type(info)
        method_map = {
            "spot": self._get_spot_tickers,
            "futures": self._get_futures_tickers,
            "perp": self._get_perp_tickers,
        }
        # prepare request params
        if market_type == "spot":
            params = {"currency_pair": info["raw_data"]["id"]}
        elif market_type == "futures":
            params = {"contract": info["raw_data"]["name"], "settle": info["settle"].lower()}
        else:  # perp
            params = {"contract": info["raw_data"]["name"], "settle": info["settle"].lower()}

        return {instrument_id: self.parser.parse_raw_ticker(await method_map[market_type](**params), market_type, info)}

    async def get_current_candlestick(self, instrument_id: str, interval: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in {self.name} exchange info")

        info = self.exchange_info[instrument_id]
        market_type = self.parser.get_market_type(info)
        _interval = self.parser.get_interval(interval)

        method_map = {
            "spot": self._get_spot_klines,
            "futures": self._get_futures_klines,
            "perp": self._get_perp_klines,
        }
        params = {
            "symbol": info["raw_data"]["id" if market_type == "spot" else "name"],
            "interval": _interval,
            "limit": 1,
        }

        if market_type in ["futures", "perp"]:
            params["settle"] = info["settle"].lower()

        return {
            instrument_id: self.parser.parse_candlesticks(
                await method_map[market_type](**params), info, market_type, interval
            )
        }

    async def get_history_candlesticks(
        self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None
    ) -> list:
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
            "spot": self._get_spot_klines,
            "futures": self._get_futures_klines,
            "perp": self._get_perp_klines,
        }

        params = {
            "symbol": info["raw_data"][symbol_map[market_type]],
            "interval": _interval,
            "limit": limit_map[market_type],
        }

        # handle special market type params
        if market_type in ["futures", "perp"]:
            params["settle"] = info["settle"].lower()

        results = []
        query_end = None
        if start and end:
            query_end = str(int(str(end)[:10]) + 1)
            while True:
                params.update({"end": query_end})
                result = self.parser.parse_candlesticks(
                    await method_map[market_type](**params), info, market_type, interval
                )
                results.extend(result)

                if len(result) < limit_map[market_type]:
                    break

                query_end = min([v["timestamp"] for v in result])

                if query_end < start:
                    break

                query_end = str(int(str(query_end)[:10]) + 1)
                continue

            return sorted(
                [v for v in results if start <= v["timestamp"] <= end], key=lambda x: x["timestamp"], reverse=False
            )

        elif num:
            while True:
                params.update({"end": query_end} if query_end else {})
                result = self.parser.parse_candlesticks(
                    await method_map[market_type](**params), info, market_type, interval
                )
                results.extend(result)

                if len(result) < limit_map[market_type] or len(results) >= num:
                    break
                query_end = str(int(str(min([v["timestamp"] for v in result]))[:10]) + 1)

                continue
            return sorted(results, key=lambda x: x["timestamp"], reverse=False)[-num:]
        else:
            raise ValueError("(start, end) or num must be provided")

    async def get_current_funding_rate(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not in {self.name} exchange info")

        info = self.exchange_info[instrument_id]
        market_type = self.parser.get_market_type(info)

        method_map = {
            "futures": self._get_futures_tickers,
            "perp": self._get_perp_tickers,
        }

        params = {
            "contract": info["raw_data"]["name"],
            "settle": info["settle"].lower(),
        }
        return {instrument_id: self.parser.parse_current_funding_rate(await method_map[market_type](**params), info)}

    async def get_history_funding_rate(
        self, instrument_id: str, start: int = None, end: int = None, num: int = None
    ) -> list:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not in {self.name} exchange info")

        info = self.exchange_info[instrument_id]

        params = {"contract": info["raw_data"]["name"], "settle": info["settle"].lower(), "limit": 1000}

        results = self.parser.parse_history_funding_rate(await self._get_futures_funding_rate_history(**params), info)

        if start and end:
            return sorted(
                [v for v in results if start <= v["timestamp"] <= end], key=lambda x: x["timestamp"], reverse=False
            )
        elif num:
            return sorted(results, key=lambda x: x["timestamp"], reverse=False)[-num:]
        else:
            raise ValueError("(start, end) or num must be provided")

    async def get_last_price(self, instrument_id: str) -> dict:
        ticker = await self.get_ticker(instrument_id)
        ticker = ticker[instrument_id]
        info = self.exchange_info[instrument_id]

        return {
            "timestamp": ticker["timestamp"],
            "instrument_id": instrument_id,
            "market_type": self.parser.parse_unified_market_type(info),
            "last_price": ticker["last"],
            "raw_data": ticker,
        }

    async def get_index_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not in {self.name} exchange info")
        info = self.exchange_info[instrument_id]
        return {
            "timestamp": self.parser.get_timestamp(),
            "instrument_id": instrument_id,
            "market_type": self.parser.parse_unified_market_type(info),
            "index_price": None,
            "raw_data": None,
        }  # gateio do not support index price

    async def get_mark_price(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not in {self.name} exchange info")

        info = self.exchange_info[instrument_id]
        return {
            "timestamp": self.parser.get_timestamp(),
            "instrument_id": instrument_id,
            "market_type": self.parser.parse_unified_market_type(info),
            "mark_price": None,
            "raw_data": None,
        }  # gateio do not support mark price
