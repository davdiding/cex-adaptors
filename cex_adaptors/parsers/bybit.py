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

    def parse_funding_rate(self, response: dict, info: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        results = []
        for data in datas:
            results.append(
                {
                    "timestamp": self.parse_str(data["fundingRateTimestamp"], int),
                    "instrument_id": self.parse_unified_id(info),
                    "funding_rate": self.parse_str(data["fundingRate"], float),
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
                {
                    "price": self.parse_str(ask[0], float),
                    "volume": self.parse_str(ask[1], float),
                }
                for ask in asks
            ],
            "bids": [
                {
                    "price": self.parse_str(bid[0], float),
                    "volume": self.parse_str(bid[1], float),
                }
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
