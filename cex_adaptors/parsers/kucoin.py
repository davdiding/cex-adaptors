from ..utils import query_dict
from .base import Parser


class KucoinParser(Parser):
    SPOT_INTERVAL_MAP = {
        "1m": "1min",
        "3m": "3min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1hour",
        "2h": "2hour",
        "4h": "4hour",
        "6h": "6hour",
        "8h": "8hour",
        "12h": "12hour",
        "1d": "1day",
        "1w": "1week",
    }

    DERIVATIVE_INTERVAL_MAP = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "8h": 480,
        "12h": 720,
        "1d": 1440,
        "1w": 10080,
    }

    @staticmethod
    def check_response(response: dict):
        if response.get("code") == "200000":
            return {"code": 200, "status": "success", "data": response["data"]}
        else:
            raise ValueError(f"Error when checking response: {response} in KucoinParser")

    def parse_kucoin_base_currency(self, base: str) -> str:
        base = base.replace("XBT", "BTC")
        return self.parse_base_currency(base)

    @property
    def spot_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["enableTrading"]),
            "is_spot": True,
            "is_margin": (lambda x: x["isMarginEnabled"]),
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(x["baseCurrency"], x["quoteCurrency"])),
            "base": (lambda x: self.parse_kucoin_base_currency(x["baseCurrency"])),
            "quote": (lambda x: str(x["quoteCurrency"])),
            "settle": (lambda x: str(x["quoteCurrency"])),
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

    @property
    def futures_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["status"] == "Open"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: True if x["expireDate"] else False),
            "is_perp": (lambda x: False if x["expireDate"] else True),
            "is_linear": (lambda x: True),  # Not yet implemented
            "is_inverse": (lambda x: False),  # Not yet implemented
            "symbol": (lambda x: self.parse_unified_symbol(x["baseCurrency"], x["quoteCurrency"])),
            "base": (lambda x: self.parse_kucoin_base_currency(x["baseCurrency"])),
            "quote": (lambda x: str(x["quoteCurrency"])),
            "settle": (lambda x: str(x["quoteCurrency"])),
            "multiplier": (lambda x: self.parse_multiplier(x["baseCurrency"])),
            "leverage": (lambda x: x["maxLeverage"]),
            "listing_time": (lambda x: x["firstOpenDate"] if x["firstOpenDate"] else None),
            "expiration_time": (lambda x: x["expireDate"] if x["expireDate"] else None),
            "contract_size": (lambda x: abs(int(x["multiplier"]))),
            "tick_size": None,  # Not yet implemented
            "min_order_size": None,  # Not yet implemented
            "max_order_size": None,  # Not yet implemented
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

    def get_id_map(self, infos: dict, market_type: str) -> dict:
        if market_type == "derivative":
            infos = query_dict(infos, f"is_futures == True or is_perp == True")
        else:
            infos = query_dict(infos, f"is_{market_type} == True")
        return {v["raw_data"]["symbol"]: k for k, v in infos.items()}

    def parse_spot_tickers(self, response: dict, infos: dict) -> dict:
        response = self.check_response(response)
        id_map = self.get_id_map(infos, "spot")

        datas = response["data"]["ticker"]
        results = {}
        for data in datas:
            id = id_map[data["symbol"]]
            result = self.parse_spot_ticker(data, infos[id])
            results[id] = result
        return results

    def parse_derivative_tickers(self, response: dict, infos: dict) -> dict:
        datas = response

        results = {}
        id_map = self.get_id_map(infos, "derivative")
        for data in datas:
            data = data["data"]
            instrument_id = id_map[data["symbol"]]
            result = self.parse_derivative_ticker(data, infos[instrument_id])
            results[instrument_id] = result
        return results

    def get_interval(self, interval: str, market: str) -> str:
        if market == "spot":
            if interval not in self.SPOT_INTERVAL_MAP:
                raise ValueError(f"Invalid interval: {interval}. Must be one of {list(self.SPOT_INTERVAL_MAP.keys())}")
            return self.SPOT_INTERVAL_MAP[interval]
        else:
            if interval not in self.DERIVATIVE_INTERVAL_MAP:
                raise ValueError(
                    f"Invalid interval: {interval}. Must be one of {list(self.DERIVATIVE_INTERVAL_MAP.keys())}"
                )
            return self.DERIVATIVE_INTERVAL_MAP[interval]

    def parse_kucoin_timestamp(self, timestamp: int, market_type: str) -> int:
        timestamp_str = str(timestamp)

        if market_type == "spot":
            if len(timestamp_str) == 13:
                return int(timestamp_str[:-3])
            elif len(timestamp_str) == 10:
                return timestamp
            else:
                raise ValueError(f"Invalid timestamp: {timestamp}. Must be in seconds or milliseconds.")
        else:
            if len(timestamp_str) == 10:
                return int(timestamp_str + "000")
            elif len(timestamp_str) == 13:
                return timestamp
            else:
                raise ValueError(f"Invalid timestamp: {timestamp}. Must be in seconds or milliseconds.")

    def parse_ticker(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"]
        if market_type == "spot":
            return self.parse_spot_ticker(data, info)
        else:
            return self.parse_derivative_ticker(data, info)

    def parse_spot_ticker(self, response: dict, info: dict) -> dict:
        data = response

        last = self.parse_str(data["last"], float)
        change_price = self.parse_str(data["changePrice"], float)
        open = last - change_price

        timestamp = self.parse_str(data["time"], int) if "time" in data else self.get_timestamp()

        return {
            "timestamp": timestamp,
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": timestamp,
            "open": open,
            "high": self.parse_str(data["high"], float),
            "low": self.parse_str(data["low"], float),
            "last": last,
            "base_volume": None,  # not yet implemented
            "quote_volume": self.parse_str(data["volValue"], float),
            "price_change": change_price,
            "price_change_percent": self.parse_str(data["changeRate"], float),
            "raw_data": response,
        }

    def parse_derivative_ticker(self, response: dict, info: dict) -> dict:
        data = response

        last = self.parse_str(data["lastTradePrice"], float)
        change_price = self.parse_str(data["priceChg"], float)
        open = last - change_price

        return {
            "timestamp": self.get_timestamp(),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": self.get_timestamp(),
            "open": open,
            "high": self.parse_str(data["highPrice"], float),
            "low": self.parse_str(data["lowPrice"], float),
            "last": last,
            "base_volume": self.parse_str(data["volumeOf24h"], float),
            "quote_volume": self.parse_str(data["turnoverOf24h"], float),
            "price_change": change_price,
            "price_change_percent": self.parse_str(data["priceChgPct"], float),
            "raw_data": response,
        }

    def parse_mark_price(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"]

        return {
            "timestamp": self.parse_str(data["timePoint"], int),
            "instrument_id": self.parse_unified_id(info),
            "mark_price": self.parse_str(data["value"], float),
            "raw_data": data,
        }

    def parse_index_price(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"]

        return {
            "timestamp": self.parse_str(data["timePoint"], int),
            "instrument_id": self.parse_unified_id(info),
            "index_price": self.parse_str(data["indexPrice"], float),
            "raw_data": data,
        }

    def parse_orderbook(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"]

        return {
            "timestamp": self.parse_str(data["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "bids": [
                {
                    "price": self.parse_str(bid[0], float),
                    "volume": self.parse_str(bid[1], float),
                    "order_number": None,
                }
                for bid in data["bids"]
            ],
            "asks": [
                {
                    "price": self.parse_str(ask[0], float),
                    "volume": self.parse_str(ask[1], float),
                    "order_number": None,
                }
                for ask in data["asks"]
            ],
            "raw_data": data,
        }

    def parse_current_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"]
        return {
            "timestamp": self.parse_str(data["timePoint"], int),
            "next_funding_time": None,
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "funding_rate": self.parse_str(data["value"], float),
            "raw_data": data,
        }

    def parse_history_funding_rate(self, response: dict, info: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        instrument_id = self.parse_unified_id(info)
        market_type = self.parse_unified_market_type(info)

        results = [
            {
                "timestamp": self.parse_str(data["timepoint"], int),
                "instrument_id": instrument_id,
                "market_type": market_type,
                "funding_rate": self.parse_str(data["fundingRate"], float),
                "realized_rate": self.parse_str(data["fundingRate"], float),
                "raw_data": data,
            }
            for data in datas
        ]

        return results

    def parse_current_candlestick(self, response: dict, info: dict, market_type: str, interval: str) -> dict:
        response = self.check_response(response)
        data = response["data"][0]

        result = self.parse_candlestick(data, info, market_type)
        result.update(
            {
                "instrument_id": self.parse_unified_id(info),
                "market_type": self.parse_unified_market_type(info),
                "interval": interval,
            }
        )
        return result

    def parse_history_candlesticks(self, response: dict, info: dict, market_type: str, interval: str) -> list:
        response = self.check_response(response)
        datas = response["data"]

        update_ = {
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "interval": interval,
        }
        results = []
        for data in datas:

            result = self.parse_candlestick(data, info, market_type)
            result.update(update_)
            results.append(result)

        return results

    def parse_candlestick(self, data: dict, info: dict, market_type) -> dict:
        return {
            "timestamp": self.parse_str(data[0], int) * (1000 if len(str(data[0])) == 10 else 1),
            "open": self.parse_str(data[1], float),
            "high": self.parse_str(data[3], float),
            "low": self.parse_str(data[4], float),
            "close": self.parse_str(data[2], float),
            "base_volume": self.parse_str(data[5], float) if market_type == "spot" else None,
            "quote_volume": self.parse_str(data[6 if market_type == "spot" else 5], float),
            "contract_volume": self.parse_str(data[5], float) if market_type == "spot" else None,
            "raw_data": data,
        }
