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
            raise {"code": 400, "status": "error", "data": response}

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
        if response["code"] != 200:
            return response

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
        if response["code"] != 200:
            return response

        id_map = self.get_id_map(infos, "spot")

        datas = response["data"]["ticker"]
        results = {}
        for data in datas:
            id = id_map[data["symbol"]]
            result = self.parse_spot_ticker(data, close_time=response["data"]["time"])
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

    def parse_derivative_ticker(self, response: dict, infos: dict) -> dict:
        return {
            "symbol": infos["symbol"],
            "open_time": None,
            "close_time": None,
            "open": None,
            "high": float(response["highPrice"]),
            "low": float(response["lowPrice"]),
            "last_price": float(response["lastTradePrice"]),
            "base_volume": float(response["volumeOf24h"]),
            "quote_volume": float(response["turnoverOf24h"]),
            "price_change": float(response["priceChg"]),
            "price_change_percent": float(response["priceChgPct"]),
            "raw_data": response,
        }

    def parse_spot_ticker(self, response: dict, close_time: int) -> dict:
        return {
            "symbol": response["symbol"],
            "open_time": None,
            "close_time": close_time,
            "open": None,
            "high": float(response["high"]),
            "low": float(response["low"]),
            "last_price": float(response["last"]),
            "base_volume": float(response["vol"]),
            "quote_volume": float(response["volValue"]),
            "price_change": float(response["changePrice"]),
            "price_change_percent": float(response["changeRate"]),
            "raw_data": response,
        }

    def parse_klines(self, response: dict, info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        results = {}
        datas = response["data"]
        for data in datas:
            timestamp = int(int(data[0]) * 1000) if len(str(data[0])) == 10 else int(data[0])
            results[timestamp] = self.parse_kline(data, info, market_type)
        return results

    def parse_kline(self, response: dict, info: dict, market_type: str) -> dict:
        if market_type == "spot":
            return {
                "open": float(response[1]),
                "high": float(response[3]),
                "low": float(response[4]),
                "close": float(response[2]),
                "base_volume": float(response[5]),
                "quote_volume": float(response[6]),
                "close_time": None,
                "raw_data": response,
            }
        else:
            return {
                "open": float(response[1]),
                "high": float(response[2]),
                "low": float(response[3]),
                "close": float(response[4]),
                "base_volume": float(response[5]),
                "quote_volume": None,
                "close_time": None,
                "raw_data": response,
            }

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
