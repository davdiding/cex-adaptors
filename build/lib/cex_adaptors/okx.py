import tracemalloc

from .exchanges.okx import OkxUnified
from .parsers.okx import OkxParser

tracemalloc.start()


class Okx(OkxUnified):
    name = "okx"
    market_type_map = {"spot": "SPOT", "margin": "MARGIN", "futures": "FUTURES", "perp": "SWAP"}
    _market_type_map = {"SPOT": "spot", "MARGIN": "margin", "FUTURES": "futures", "SWAP": "perp"}

    def __init__(self, api_key: str = None, api_secret: str = None, passphrase: str = None, flag: str = "1"):
        super().__init__(api_key=api_key, api_secret=api_secret, passphrase=passphrase, flag=flag)

        self.parser = OkxParser()
        self.exchange_info = {}

    async def sync_exchange_info(self):
        self.exchange_info = await self.get_exchange_info()

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

    async def get_tickers(self, market_type: str = None) -> dict:

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

    async def get_balance(self):
        return self.parser.parse_balance(await self._get_balance())

    async def get_positions(self):
        return self.parser.parse_positions(await self._get_positions(), self.exchange_info)

    async def get_account_info(self):
        return self.parser.parse_account_config(await self._get_account_config())

    async def place_market_order(self, instrument_id: str, side: str, volume: float, in_quote: bool = False):
        if instrument_id not in self.exchange_info:
            raise Exception(f"{instrument_id} not found in exchange_info")

        info = self.exchange_info[instrument_id]
        _instrument_id = info["raw_data"]["instId"]
        _order_type = "market"

        order_id = self.parser.parse_order_id(
            await self._place_order(
                instId=_instrument_id,
                side=side,
                sz=str(volume),
                ordType=_order_type,
                tgtCcy="quote_ccy" if in_quote else "base_ccy",
            )
        )
        return self.parser.parse_order_info(await self._get_order_info(_instrument_id, str(order_id)), info)

    async def place_limit_order(
        self, instrument_id: str, side: str, price: float, volume: float, in_quote: bool = False
    ):
        if instrument_id not in self.exchange_info:
            raise Exception(f"{instrument_id} not found in exchange_info")

        info = self.exchange_info[instrument_id]
        _instrument_id = info["raw_data"]["instId"]
        _order_type = "limit"

        order_id = self.parser.parse_order_id(
            await self._place_order(
                instId=_instrument_id,
                side=side,
                sz=str(volume),
                px=str(price),
                ordType=_order_type,
                tgtCcy="quote_ccy" if in_quote else "base_ccy",
            )
        )
        return self.parser.parse_order_info(await self._get_order_info(_instrument_id, str(order_id)), info)

    async def cancel_order(self, instrument_id: str, order_id: int):
        if instrument_id not in self.exchange_info:
            raise Exception(f"{instrument_id} not found in exchange_info")
        info = self.exchange_info[instrument_id]
        _instrument_id = info["raw_data"]["instId"]
        return self.parser.parse_cancel_order(await self._cancel_order(_instrument_id, str(order_id)))

    async def get_opened_orders(self, market_type: str = None, instrument_id: str = None) -> list:
        results = []
        params = {"limit": "100"}
        if market_type:
            _market_type = self.market_type_map[market_type]
            params["instType"] = _market_type
            results = self.parser.parse_opened_orders(
                await self._get_opended_orders(**params), infos=self.exchange_info
            )
        elif instrument_id:
            if instrument_id not in self.exchange_info:
                raise Exception(f"{instrument_id} not found in exchange_info")
            info = self.exchange_info[instrument_id]
            _instrument_id = info["raw_data"]["instId"]
            params["instId"] = _instrument_id
            results = self.parser.parse_opened_orders(await self._get_opended_orders(**params), info=info)
        else:
            raise Exception("market_type or instrument_id must be provided")

        return results

    async def get_history_orders(self, market_type: str = None, instrument_id: str = None) -> list:
        results = []
        params = {"limit": "100"}

        if market_type:
            pass
        elif instrument_id:
            if instrument_id not in self.exchange_info:
                raise Exception
            info = self.exchange_info[instrument_id]
            _instrument_id = info["raw_data"]["instId"]
            _market = info["raw_data"]["instType"]
            params.update({"instId": _instrument_id, "instType": _market})
            results = self.parser.parse_history_orders(
                await self._get_history_orders(**params), infos=self.exchange_info
            )

        else:
            raise Exception("market_type or instrument_id must be provided")

        return results

    async def get_history_funding_rate(
        self, instrument_id: str, start: int = None, end: int = None, num: int = 30
    ) -> list:
        if instrument_id not in self.exchange_info:
            raise Exception(f"{instrument_id} not found in exchange_info")

        info = self.exchange_info[instrument_id]
        _instrument_id = info["raw_data"]["instId"]
        limit = 100
        params = {"instId": _instrument_id, "limit": limit}
        results = []
        query_end = None

        if start and end:
            query_end = end + 1
            while True:
                params.update({"after": query_end})
                result = self.parser.parse_funding_rates(
                    await self._get_history_funding_rate(**params),
                    info,
                )
                results.extend(result)
                if not result or len(result) < limit:
                    break
                query_end = min([v["timestamp"] for v in result])

                if query_end < start:
                    break
                continue
            return [v for v in results if start <= v["timestamp"] <= end]

        elif num:
            while True:
                params.update({"after": query_end} if query_end else {})
                result = self.parser.parse_funding_rates(
                    await self._get_history_funding_rate(**params),
                    info,
                )
                results.extend(result)

                # exclude same timestamp
                results = list({v["timestamp"]: v for v in results}.values())

                if not result or len(result) < limit or len(results) >= num:
                    break

                query_end = min([v["timestamp"] for v in result])
                continue
            return sorted(results, key=lambda x: x["timestamp"], reverse=False)[-num:]

        else:
            raise Exception("(start, end) or num must be provided")

    async def get_current_funding_rate(self, instrument_id: str) -> dict:
        if instrument_id not in self.exchange_info:
            raise Exception(f"{instrument_id} not found in exchange_info")
        info = self.exchange_info[instrument_id]
        _instrument_id = info["raw_data"]["instId"]
        return self.parser.parse_current_funding_rate(await self._get_current_funding_rate(_instrument_id), info)
