from .base import Parser


class BybitParser(Parser):
    INTERVAL_MAP = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "6h": "360",
        "12h": "720",
        "1d": "D",
        "1M": "M",
        "1w": "W",
    }

    def check_response(self, response: dict):
        if response["retCode"] != 0:
            raise ValueError(f"Error in parsing Bybit response: {response}")
        else:
            return {
                "code": 200,
                "status": "success",
                "data": response["result"]["list"] if "list" in response["result"] else response["result"],
                "timestamp": self.parse_str(response["time"], int),
            }

    @staticmethod
    def get_market_type(info: dict) -> str:
        if info["is_spot"]:
            return "spot"
        elif info["is_linear"]:
            return "linear"
        elif info["is_inverse"]:
            return "inverse"
        else:
            raise ValueError(f"Invalid market type: {info}")

    @property
    def spot_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["status"] == "Trading"),
            "is_spot": True,
            "is_margin": (lambda x: self.parse_is_margin(x["marginTrading"])),
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(x["baseCoin"], x["quoteCoin"])),
            "base": (lambda x: str(x["baseCoin"])),
            "quote": (lambda x: str(x["quoteCoin"])),
            "settle": (lambda x: str(x["quoteCoin"])),
            "multiplier": 1,  # spot multiplier default 1
            "leverage": 1,  # spot leverage default 1
            "listing_time": None,  # api not support this field
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,  # spot contract size default 1
            "tick_size": (lambda x: float(x["priceFilter"]["tickSize"])),
            "min_order_size": (lambda x: float(x["lotSizeFilter"]["minOrderQty"])),
            "max_order_size": (lambda x: float(x["lotSizeFilter"]["maxOrderQty"])),
            "raw_data": (lambda x: x),
        }

    @property
    def perp_futures_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["status"] == "Trading"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: self.parse_is_futures(x["contractType"])),
            "is_perp": (lambda x: self.parse_is_perpetual(x["contractType"])),
            "is_linear": (lambda x: self.parse_is_linear(x["contractType"])),
            "is_inverse": (lambda x: self.parse_is_inverse(x["contractType"])),
            "symbol": (lambda x: self.parse_unified_symbol(self.parse_base_currency(x["baseCoin"]), x["quoteCoin"])),
            "base": (lambda x: self.parse_base_currency(x["baseCoin"])),
            "quote": (lambda x: str(x["quoteCoin"])),
            "settle": (lambda x: str(x["settleCoin"])),
            "multiplier": (lambda x: self.parse_multiplier(x["baseCoin"])),
            "leverage": (lambda x: float(x["leverageFilter"]["maxLeverage"])),
            "listing_time": (lambda x: int(x["launchTime"])),
            "expiration_time": (lambda x: int(x["deliveryTime"])),
            "contract_size": (lambda x: float(x["lotSizeFilter"]["qtyStep"])),
            "tick_size": (lambda x: float(x["priceFilter"]["tickSize"])),
            "min_order_size": (lambda x: float(x["lotSizeFilter"]["minOrderQty"])),
            "max_order_size": (lambda x: float(x["lotSizeFilter"]["maxOrderQty"])),
            "raw_data": (lambda x: x),
        }

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        results = {}
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results

    def parse_tickers(self, response: dict, market_type: str, infos: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        id_map = self.get_id_symbol_map(infos, market_type)
        results = {}
        for data in datas:
            symbol = data["symbol"]
            if symbol not in id_map:
                print(f"Symbol {symbol} not found in Bybit exchange info.")
                continue
            instrument_id = id_map[symbol]
            results[instrument_id] = self.parse_ticker(
                data, market_type, infos[instrument_id], timestamp=response["timestamp"]
            )
        return results

    def parse_raw_ticker(self, response: dict, market_type: str, info: dict):
        response = self.check_response(response)
        data = response["data"][0]
        return self.parse_ticker(data, market_type, info, timestamp=response["timestamp"])

    def parse_ticker(self, response: dict, market_type: str, info: dict, **kwargs) -> dict:
        return {
            "timestamp": self.parse_str(kwargs["timestamp"], int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": self.parse_str(kwargs["timestamp"], int),
            "open": self.parse_str(response["prevPrice24h"], float),
            "high": self.parse_str(response["highPrice24h"], float),
            "low": self.parse_str(response["lowPrice24h"], float),
            "last": self.parse_str(response["lastPrice"], float),
            "base_volume": self.parse_str(response["volume24h" if market_type != "inverse" else "turnover24h"], float),
            "quote_volume": self.parse_str(response["turnover24h" if market_type != "inverse" else "volume24h"], float),
            "price_change": (
                self.parse_str(response["prevPrice24h"], float) - self.parse_str(response["lastPrice"], float)
            ),
            "price_change_percent": self.parse_str(response["price24hPcnt"], float),
            "raw_data": response,
        }

    def get_interval(self, interval: str) -> str:
        if interval not in self.INTERVAL_MAP:
            raise ValueError(f"Invalid interval: {interval}")
        return self.INTERVAL_MAP[interval]

    def parse_candlesticks(self, response: dict, info: dict, market_type: str, interval: str) -> any:
        response = self.check_response(response)
        datas = response["data"]

        market = self.parse_unified_market_type(info)
        instrument_id = self.parse_unified_id(info)

        results = [
            {
                "timestamp": self.parse_str(data[0], int),
                "instrument_id": instrument_id,
                "market_type": market,
                "interval": interval,
                "open": self.parse_str(data[1], float),
                "high": self.parse_str(data[2], float),
                "low": self.parse_str(data[3], float),
                "close": self.parse_str(data[4], float),
                "base_volume": self.parse_str(data[5], float),
                "quote_volume": self.parse_str(data[6], float),
                "contract_volume": (
                    self.parse_str(data[5], float) / (1 if market_type == "spot" else info["contract_size"])
                ),
                "raw_data": data,
            }
            for data in datas
        ]

        return results if len(results) > 1 else results[0]

    def get_category(self, info: dict) -> str:
        if info["is_spot"] or info["is_margin"]:
            return "spot"
        elif info["is_linear"]:
            return "linear"
        elif info["is_inverse"]:
            return "inverse"
        else:
            raise ValueError(f"Invalid market type: {info}")

    def parse_funding_rate(self, response: dict, info: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        instrument_id = self.parse_unified_id(info)
        market_type = self.parse_unified_market_type(info)

        results = []
        for data in datas:
            results.append(
                {
                    "timestamp": self.parse_str(data["fundingRateTimestamp"], int),
                    "instrument_id": instrument_id,
                    "market_type": market_type,
                    "funding_rate": self.parse_str(data["fundingRate"], float),
                    "realized_rate": self.parse_str(data["fundingRate"], float),
                    "raw_data": data,
                }
            )
        return results

    @staticmethod
    def get_open_interest_interval(interval: str) -> str:
        interval_map = {
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "1h",
            "4h": "4h",
            "1d": "1d",
        }
        if interval not in interval_map:
            raise ValueError(f"Invalid interval: {interval}, should be one of {list(interval_map.keys())}")
        return interval_map[interval]

    def parse_open_interest(self, response: dict, info: dict) -> dict | list:
        response = self.check_response(response)

        results = []
        datas = response["data"]
        for datas in datas:
            results.append(
                {
                    "timestamp": self.parse_str(datas["timestamp"], int),
                    "instrument_id": self.parse_unified_id(info),
                    "open_interest": self.parse_str(datas["openInterest"], float),
                    "raw_data": datas,
                }
            )
        return results[0] if len(results) == 1 else results

    def parse_orderbook(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        asks = datas["a"]
        bids = datas["b"]

        return {
            "timestamp": self.parse_str(datas["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "asks": [
                {"price": self.parse_str(ask[0], float), "volume": self.parse_str(ask[1], float), "order_number": None}
                for ask in asks
            ],
            "bids": [
                {"price": self.parse_str(bid[0], float), "volume": self.parse_str(bid[1], float), "order_number": None}
                for bid in bids
            ],
            "raw_data": datas,
        }

    def parse_last_price(self, response: dict, instrument_id: str) -> dict:
        response = self.check_response(response)
        datas = response["data"][0]
        return {
            "timestamp": response["timestamp"],
            "instrument_id": instrument_id,
            "last_price": float(datas["lastPrice"]),
            "raw_data": datas,
        }

    def parse_index_price(self, response: dict, instrument_id: str) -> dict:
        response = self.check_response(response)
        datas = response["data"][0]
        return {
            "timestamp": response["timestamp"],
            "instrument_id": instrument_id,
            "index_price": float(datas["indexPrice"]),
            "raw_data": datas,
        }

    def parse_mark_price(self, response: dict, instrument_id: str) -> dict:
        response = self.check_response(response)
        datas = response["data"][0]
        return {
            "timestamp": response["timestamp"],
            "instrument_id": instrument_id,
            "mark_price": float(datas["markPrice"]),
            "raw_data": datas,
        }

    def parse_current_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "timestamp": response["timestamp"],
            "next_funding_time": self.parse_str(data["nextFundingTime"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "funding_rate": self.parse_str(data["fundingRate"], float),
            "raw_data": data,
        }
