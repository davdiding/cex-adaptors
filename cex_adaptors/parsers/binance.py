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
            "symbol": response["symbol"],
            "open_time": int(response["openTime"]),
            "close_time": int(response["closeTime"]),
            "open": float(response["openPrice"]),
            "high": float(response["highPrice"]),
            "low": float(response["lowPrice"]),
            "last_price": float(response["lastPrice"]),
            "base_volume": base_volume,
            "quote_volume": quote_volume,
            "price_change": float(response["priceChange"]),
            "price_change_percent": float(response["priceChangePercent"]) / 100,
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
                "timestamp": self.parse_str(data["fundingTime"], int),
                "instrument_id": self.parse_unified_id(info),
                "market": self.parse_unified_market_type(info),
                "funding_rate": self.parse_str(data["fundingRate"], float),
                "realized_rate": None,
                "raw_data": data,
            }
            results.append(result)
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
