from ..utils import query_dict
from .base import Parser


class BitgetParser(Parser):
    LINEAR_FUTURES_SETTLE = ["USDT", "USDC"]

    INTERVAL_MAP = {
        "spot": {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "1h",
            "4h": "4h",
            "6h": "6h",
            "12h": "12h",
            "1d": "1day",
            "3d": "3day",
            "1w": "1week",
            "1M": "1M",
        },
        "derivative": {
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1H",
            "4h": "4H",
            "6h": "6H",
            "12h": "12H",
            "1d": "1D",
            "1w": "1W",
            "1M": "1M",
        },
    }

    def __init__(self):
        super().__init__()

    def check_response(self, response: dict):
        if response["code"] != "00000":
            return {"code": 400, "status": "error", "data": response}
        else:
            return {"code": 200, "status": "success", "data": response["data"]}

    @property
    def spot_exchange_info_parser(self):
        return {
            "active": (lambda x: x["status"] == "online"),
            "is_spot": True,
            "is_margin": False,
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
            "tick_size": None,  # Not yet implemented
            "min_order_size": (lambda x: float(x["minTradeAmount"])),
            "max_order_size": (lambda x: float(x["maxTradeAmount"])),
            "raw_data": (lambda x: x),
        }

    @property
    def derivative_exchange_info_parser(self):
        return {
            "active": (lambda x: x["symbolStatus"] == "normal"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: self.parse_is_futures(x["symbolType"])),
            "is_perp": (lambda x: self.parse_is_perpetual(x["symbolType"])),
            "is_linear": (lambda x: False if x["quoteCoin"] in self.FIAT_CURRENCY else True),
            "is_inverse": (lambda x: True if x["quoteCoin"] in self.FIAT_CURRENCY else False),
            "symbol": (lambda x: self.parse_unified_symbol(x["baseCoin"], x["quoteCoin"])),
            "base": (lambda x: self.parse_base_currency(x["baseCoin"])),
            "quote": (lambda x: str(x["quoteCoin"])),
            "settle": (lambda x: str(x["quoteCoin"])),
            "multiplier": (lambda x: self.parse_multiplier(x["baseCoin"])),
            "leverage": (lambda x: int(x["maxLever"])),
            "listing_time": (lambda x: int(x["launchTime"]) if x["launchTime"] else None),
            "expiration_time": (lambda x: int(x["deliveryTime"]) if x["deliveryTime"] else None),
            "contract_size": (lambda x: float(x["sizeMultiplier"])),
            "tick_size": None,  # not yet implemented
            "min_order_size": None,  # API not support this field
            "max_order_size": None,  # API not support this field
            "raw_data": (lambda x: x),
        }

    def parse_exchange_info(self, response: dict, parser: callable):
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        datas = response["data"]
        results = {}
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            instrument_id = self.parse_unified_id(result)
            results[instrument_id] = result
        return results

    def get_bitget_id_map(self, exchange_info: dict, market_type: str) -> dict:
        if market_type == "derivative":
            infos = query_dict(exchange_info, "is_perp == True or is_futures == True")
        else:
            infos = query_dict(exchange_info, f"is_{market_type} == True")

        return {v["raw_data"]["symbol"]: k for k, v in infos.items()}

    def parse_spot_ticker(self, response: dict, info: dict):
        return {
            "symbol": response["symbol"],
            "open_time": None,  # API not support
            "close_time": int(response["ts"]),
            "open": float(response["open"]),
            "high": float(response["high24h"]),
            "low": float(response["low24h"]),
            "last_price": float(response["lastPr"]),
            "base_volume": float(response["baseVolume"]),
            "quote_volume": float(response["quoteVolume"]),
            "price_change": float(response["change24h"]),
            "price_change_percent": None,  # API not support
            "raw_data": response,
        }

    def parse_derivative_ticker(self, response: dict, info: dict):
        return {
            "symbol": response["symbol"],
            "open_time": None,  # API not support
            "close_time": int(response["ts"]),
            "open": float(response["open24h"]),
            "high": float(response["high24h"]),
            "low": float(response["low24h"]),
            "last_price": float(response["lastPr"]),
            "base_volume": float(response["baseVolume"]),
            "quote_volume": float(response["quoteVolume"]),
            "price_change": float(response["change24h"]),
            "price_change_percent": None,  # API not support
            "raw_data": response,
        }

    def parse_tickers(self, response: dict, exchange_info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        datas = response["data"]

        method_map = {"spot": self.parse_spot_ticker, "derivative": self.parse_derivative_ticker}

        id_map = self.get_bitget_id_map(exchange_info, market_type)
        results = {}
        for data in datas:
            if data["symbol"] not in id_map:
                print(f"Unmapped symbol: {data['symbol']} in {market_type}")
                continue
            instrument_id = id_map[data["symbol"]]
            results[instrument_id] = method_map[market_type](data, exchange_info[instrument_id])
        return results

    @staticmethod
    def get_market_type(response: dict) -> str:
        if response["is_spot"]:
            return "spot"
        elif response["is_futures"] or response["is_perp"]:
            return "derivative"
        else:
            raise ValueError("Unknown market type")

    @staticmethod
    def get_product_type(response: dict) -> str:
        if response["is_linear"]:
            return f"{response['settle']}-FUTURES"
        else:
            return "COIN-FUTURES"

    def get_interval(self, interval: str, market_type: str) -> str:
        if interval not in self.INTERVAL_MAP[market_type]:
            raise ValueError(f"Invalid interval {interval}")
        return self.INTERVAL_MAP[market_type][interval]

    def parse_klines(self, response: dict, market_type: str) -> dict:
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        datas = response["data"]
        results = {}
        for data in datas:
            timestamp = int(data[0])
            results[timestamp] = self.parse_kline(data, market_type)
        return results

    def parse_kline(self, response: list, market_type: str) -> dict:
        return {
            "open": float(response[1]),
            "high": float(response[2]),
            "low": float(response[3]),
            "close": float(response[4]),
            "base_volume": float(response[5]),
            "quote_volume": float(response[6]),
            "close_time": None,
            "raw_data": response,
        }
