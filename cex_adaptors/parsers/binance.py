from .base import Parser


class BinanceParser(Parser):
    @staticmethod
    def check_response(response: dict):
        return {"code": 200, "status": "success", "data": response}

    @property
    def spot_exchange_info_parser(self):
        return {
            "active": (lambda x: x["status"] == "TRADING"),
            "is_spot": True,
            "is_margin": (lambda x: x["isMarginTradingAllowed"]),
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(x["baseAsset"], x["quoteAsset"])),
            "base": (lambda x: str(x["baseAsset"])),
            "quote": (lambda x: str(x["quoteAsset"])),
            "settle": (lambda x: str(x["quoteAsset"])),
            "multiplier": 1,  # spot multiplier default 1
            "leverage": 1,  # spot leverage default 1
            "listing_time": None,  # api not support this field
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,  # spot contract size default 1
            "tick_size": None,  # Not yet implemented
            "min_order_size": None,  # Not yet implemented
            "max_order_size": None,  # Not yet implemented
            "raw_data": (lambda x: x),
        }

    def futures_exchange_info_parser(self, market_type: str):
        return {
            "active": (lambda x: (x["status"] if market_type != "inverse" else x["contractStatus"]) == "TRADING"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: self.parse_is_futures(x["contractType"])),
            "is_perp": (lambda x: self.parse_is_perpetual(x["contractType"])),
            "is_linear": True if market_type == "linear" else False,
            "is_inverse": True if market_type == "inverse" else False,
            "symbol": (lambda x: self.parse_unified_symbol(self.parse_base_currency(x["baseAsset"]), x["quoteAsset"])),
            "base": (lambda x: self.parse_base_currency(x["baseAsset"])),
            "quote": (lambda x: x["quoteAsset"]),
            "settle": (lambda x: x["marginAsset"]),
            "multiplier": (lambda x: self.parse_multiplier(x["baseAsset"])),
            "leverage": 1,  # need to find another way to get the leverage data
            "listing_time": (lambda x: int(x["onboardDate"])),
            "expiration_time": (lambda x: int(x["deliveryDate"])),
            "contract_size": (
                lambda x: 1 if "contractSize" not in x else float(x["contractSize"])
            ),  # binance only have contract size to inverse perp and futures
            "tick_size": None,  # not yet implemented
            "min_order_size": None,  # not yet implemented
            "max_order_size": None,  # not yet implemented
            "raw_data": (lambda x: x),
        }

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        datas = response["data"]["symbols"]
        results = {}
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result

        return results

    def parse_ticker(self, response: dict, info: dict) -> dict:
        if isinstance(response, list):
            response = response[0]

        base_volume = float(response["volume"] if info["is_linear"] else response["baseVolume"])
        quote_volume = float(response["quoteVolume"] if info["is_linear"] else response["volume"])

        quote_volume *= info["contract_size"] if info["is_perp"] or info["is_futures"] else 1

        return {
            "timestamp": self.parse_str(response["closeTime"], int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": self.parse_str(response["openTime"], int),
            "close_time": self.parse_str(response["closeTime"], int),
            "open": self.parse_str(response["openPrice"], float),
            "high": self.parse_str(response["highPrice"], float),
            "low": self.parse_str(response["lowPrice"], float),
            "last": self.parse_str(response["lastPrice"], float),
            "base_volume": base_volume,
            "quote_volume": quote_volume,
            "price_change": self.parse_str(response["priceChange"], float),
            "price_change_percent": self.parse_str(response["priceChangePercent"], float) / 100,
            "raw_data": response,
        }

    def get_id_map(self, infos: dict, market_type: str) -> dict:
        infos = self.query_dict(infos, {f"is_{market_type}": True})
        return {v["raw_data"]["symbol"]: k for k, v in infos.items()}

    def parse_tickers(self, response: dict, market_type: str, infos: dict) -> dict:
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        datas = response["data"]
        id_map = self.get_id_map(infos, market_type)
        results = {}
        for data in datas:
            if data["symbol"] not in id_map:
                print(data["symbol"])
                continue
            instrument_id = id_map[data["symbol"]]
            results[instrument_id] = self.parse_ticker(data, infos[instrument_id])
        return results

    def parse_kline(self, response: list, market_type: str) -> dict:
        """
        # spot
        [
            [
                1499040000000,      // Kline open time
                "0.01634790",       // Open price
                "0.80000000",       // High price
                "0.01575800",       // Low price
                "0.01577100",       // Close price
                "148976.11427815",  // Volume
                1499644799999,      // Kline Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "0"                 // Unused field, ignore.
            ]
        ]

        # linear
        # inverse
        """
        return {
            int(response[0]): {
                "open": float(response[1]),
                "high": float(response[2]),
                "low": float(response[3]),
                "close": float(response[4]),
                "base_volume": float(response[7]),
                "quote_volume": float(response[5]),
                "close_time": int(response[6]),
                "raw_data": response,
            }
        }

    def parse_klines(self, response: list, market_type: str) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        results = {}
        for data in datas:
            result = self.parse_kline(data, market_type)
            results.update(result)
        return results

    def parse_spot_account_info(self, response: dict) -> dict:
        response = self.check_response(response)
        data = response["data"]
        return {
            "timestamp": data["updateTime"],
            "account_id": data["uid"],
            "account_type": data["accountType"],
            "raw_data": data,
        }

    def parse_margin_account_info(self, response: dict) -> dict:
        response = self.check_response(response)

        data = response["data"]
        return {"raw_data": data}

    def parse_margin_balance(self, response: dict, currency: str = None) -> dict:
        response = self.check_response(response)
        datas = response["data"]["userAssets"]
        query_currency = currency

        results = {}
        for data in datas:
            if data["netAsset"] == "0":
                continue

            result = {
                "currency": data["asset"],
                "balance": self.parse_str(data["netAsset"], float),
                "available": self.parse_str(data["free"], float),
                "raw_data": data,
            }
            currency = result["currency"]
            results[currency] = result
        return results if not query_currency else {query_currency: results[query_currency]}

    def parse_history_funding_rate(self, response: dict, info: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        results = []
        for data in datas:
            result = {
                "timestamp": int(round(self.parse_str(data["fundingTime"], int) / 1000)) * 1000,
                "instrument_id": self.parse_unified_id(info),
                "market_type": self.parse_unified_market_type(info),
                "funding_rate": self.parse_str(data["fundingRate"], float),
                "realized_rate": None,
                "raw_data": data,
            }
            results.append(result)
        return results

    def parse_last_price(self, response: dict, instrument_id: str) -> dict:
        response = self.check_response(response)
        data = response["data"][instrument_id]

        return {
            "timestamp": data["close_time"],
            "instrument_id": instrument_id,
            "last_price": self.parse_str(data["last_price"], float),
            "raw_data": data,
        }

    def parse_index_price(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"]

        if not isinstance(data, dict):
            data = data[0]

        if market_type == "spot":
            return {
                "timestamp": self.parse_str(data["calcTime"], int),
                "instrument_id": self.parse_unified_id(info),
                "index_price": self.parse_str(data["price"], float),
                "raw_data": data,
            }
        else:  # linear, inverse
            return {
                "timestamp": self.parse_str(data["time"], int),
                "instrument_id": self.parse_unified_id(info),
                "index_price": self.parse_str(data["indexPrice"], float),
                "raw_data": data,
            }

    def parse_mark_price(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"]

        if not isinstance(data, dict):
            data = data[0]

        return {
            "timestamp": self.parse_str(data["time"], int),
            "instrument_id": self.parse_unified_id(info),
            "mark_price": self.parse_str(data["markPrice"], float),
            "raw_data": data,
        }

    def parse_open_interest(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"]

        return {
            "timestamp": self.parse_str(data["time"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "oi_contract": self.parse_str(data["openInterest"], float),
            "oi_currency": None,
            "raw_data": data,
        }

    def parse_orderbook(self, response: dict, info: dict, market_type: str, depth: int) -> dict:
        response = self.check_response(response)
        data = response["data"]

        results = {
            "timestamp": self.get_timestamp(),
            "instrument_id": self.parse_unified_id(info),
            "asks": [
                {
                    "price": self.parse_str(ask[0], float),
                    "volume": self.parse_str(ask[1], float),
                    "order_number": None,
                }
                for ask in data["asks"]
            ],
            "bids": [
                {
                    "price": self.parse_str(bid[0], float),
                    "volume": self.parse_str(bid[1], float),
                    "order_number": None,
                }
                for bid in data["bids"]
            ],
            "raw_data": data,
        }

        results["bids"] = sorted(results["bids"], key=lambda x: x["price"], reverse=True)
        results["asks"] = sorted(results["asks"], key=lambda x: x["price"])

        if depth:
            results["bids"] = results["bids"][:depth]
            results["asks"] = results["asks"][:depth]

        return results

    def get_symbol(self, info: dict) -> str:
        return f'{info["base"]}{info["quote"]}'

    def get_interval(self, interval: str) -> str:
        return interval

    def get_market_type(self, info: dict):
        if info["is_spot"]:
            return "spot"
        elif info["is_linear"]:
            return "linear"
        elif info["is_inverse"]:
            return "inverse"
        else:
            raise Exception("market type not found")

    def parse_current_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"]

        return {
            "timestamp": self.parse_str(data["time"], int),
            "next_funding_time": self.parse_str(data["nextFundingTime"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "funding_rate": self.parse_str(data["lastFundingRate"], float),
            "raw_data": data,
        }

    def parse_candlesticks(self, response: dict, info: dict, market_type: str, interval: str) -> any:
        response = self.check_response(response)
        datas = response["data"]
        instrument_id = self.parse_unified_id(info)
        market_type = self.parse_unified_market_type(info)

        results = []

        for data in datas:
            results.append(
                {
                    "timestamp": self.parse_str(data[0], int),
                    "instrument_id": instrument_id,
                    "market_type": market_type,
                    "interval": interval,
                    "open": self.parse_str(data[1], float),
                    "high": self.parse_str(data[2], float),
                    "low": self.parse_str(data[3], float),
                    "close": self.parse_str(data[4], float),
                    "base_volume": self.parse_str(data[5], float),
                    "quote_volume": self.parse_str(data[7], float),
                    "contract_volume": self.parse_str(data[5], float),
                    "raw_data": data,
                }
            )
        return results[0] if len(results) == 1 else results
