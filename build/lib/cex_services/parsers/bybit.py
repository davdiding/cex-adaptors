from datetime import datetime as dt
from datetime import timedelta as td

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

    @staticmethod
    def check_response(response: dict):
        if response["retCode"] != 0:
            return {
                "code": 400,
                "status": "error",
                "data": response,
            }
        else:
            return {
                "code": 200,
                "status": "success",
                "data": response["result"]["list"],
            }

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

    def parse_tickers(self, response: dict, market_type: str) -> list:
        response = self.check_response(response)
        datas = response["data"]

        results = []
        for data in datas:
            result = self.parse_ticker(data, market_type)
            results.append(result)
        return results

    def parse_ticker(self, response: dict, market_type: str) -> dict:
        return {
            "symbol": response["symbol"],
            "open_time": int((dt.now() - td(days=1)).timestamp() * 1000),
            "close_time": int(dt.now().timestamp() * 1000),
            "open": float(response["prevPrice24h"]),
            "high": float(response["highPrice24h"]),
            "low": float(response["lowPrice24h"]),
            "last_price": float(response["lastPrice"]),
            "base_volume": float(response["volume24h"]) if market_type != "inverse" else float(response["turnover24h"]),
            "quote_volume": float(response["turnover24h"])
            if market_type != "inverse"
            else float(response["volume24h"]),
            "price_change": float(response["prevPrice24h"]) - float(response["lastPrice"]),
            "price_change_percent": float(response["price24hPcnt"]),
            "raw_data": response,
        }

    def get_interval(self, interval: str) -> str:
        if interval not in self.INTERVAL_MAP:
            raise ValueError(f"Invalid interval: {interval}")
        return self.INTERVAL_MAP[interval]

    def parse_klines(self, response: dict) -> dict:
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        results = {}
        datas = response["data"]
        for data in datas:
            result = self.parse_kline(data)
            timestamp = int(data[0])
            results[timestamp] = result
        return results

    def get_category(self, info: dict) -> str:
        if info["is_spot"] or info["is_margin"]:
            return "spot"
        elif info["is_linear"]:
            return "linear"
        elif info["is_inverse"]:
            return "inverse"
        else:
            raise ValueError(f"Invalid market type: {info}")

    def parse_kline(self, response: dict) -> dict:
        return {
            "open": float(response[1]),
            "high": float(response[2]),
            "low": float(response[3]),
            "close": float(response[4]),
            "base_volume": float(response[5]),
            "quote_volume": float(response[6]),
            "raw_data": response,
        }
