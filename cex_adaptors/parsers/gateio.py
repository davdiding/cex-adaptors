from .base import Parser


class GateioParser(Parser):

    INTERVAL_MAP = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "8h": "8h",
        "1d": "1d",
        "7d": "7d",
        "1M": "30d",
    }

    def __init__(self):
        super().__init__()

    @staticmethod
    def check_response(response: dict):
        return {"code": 200, "status": "success", "data": response}

    @property
    def spot_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["trade_status"] == "tradable"),
            "is_spot": True,
            "is_margin": False,
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(x["base"], x["quote"])),
            "base": (lambda x: str(x["base"])),
            "quote": (lambda x: str(x["quote"])),
            "settle": (lambda x: str(x["quote"])),
            "multiplier": 1,  # spot multiplier default 1
            "leverage": 1,  # spot leverage default 1
            "listing_time": (lambda x: min([int(x["sell_start"]), int(x["buy_start"])]) * 1000),
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,  # spot contract size default 1
            "tick_size": 1,
            "min_order_size": (lambda x: float(x["min_quote_amount"]) if "min_quote_amount" in x else 0),
            "max_order_size": (lambda x: float(x["max_quote_amount"]) if "max_quote_amount" in x else 0),
            "raw_data": (lambda x: x),
        }

    @property
    def perp_exchange_info_parser(self) -> dict:
        return {
            "active": True,
            "is_spot": False,
            "is_margin": False,
            "is_futures": False,
            "is_perp": True,
            "is_linear": (lambda x: True if x["settle"] in self.STABLE_CURRENCY else False),
            "is_inverse": (lambda x: False if x["settle"] in self.STABLE_CURRENCY else True),
            "symbol": (
                lambda x: self.parse_unified_symbol(
                    self.parse_perp_name(x["name"])["base"], self.parse_perp_name(x["name"])["quote"]
                )
            ),
            "base": (lambda x: self.parse_perp_name(x["name"])["base"]),
            "quote": (lambda x: self.parse_perp_name(x["name"])["quote"]),
            "settle": (lambda x: str(x["settle"])),
            "multiplier": None,
            "leverage": (lambda x: float(x["leverage_max"])),
            "listing_time": None,
            "expiration_time": None,
            "contract_size": None,
            "tick_size": None,
            "min_order_size": None,
            "max_order_size": None,
            "raw_data": (lambda x: x),
        }

    @property
    def futures_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: not x["in_delisting"]),
            "is_spot": False,
            "is_margin": False,
            "is_futures": True,
            "is_perp": False,
            "is_linear": (lambda x: True if x["settle"] in self.STABLE_CURRENCY else False),
            "is_inverse": (lambda x: False if x["settle"] in self.STABLE_CURRENCY else True),
            "symbol": (
                lambda x: self.parse_unified_symbol(
                    self.parse_futures_name(x["name"])["base"], self.parse_futures_name(x["name"])["quote"]
                )
            ),
            "base": (lambda x: self.parse_base_currency(self.parse_futures_name(x["name"])["base"])),
            "quote": (lambda x: self.parse_futures_name(x["name"])["quote"]),
            "settle": (lambda x: str(x["settle"])),
            "multiplier": (lambda x: self.parse_multiplier(self.parse_futures_name(x["name"])["base"])),
            "leverage": 1,  # Not yet implemented
            "listing_time": None,
            "expiration_time": (lambda x: int(x["expire_time"]) * 1000),
            "contract_size": None,
            "tick_size": None,
            "min_order_size": None,
            "max_order_size": None,
            "raw_data": (lambda x: x),
        }

    def parse_perp_name(self, name: str) -> dict:
        return {
            "base": name.split("_")[0],
            "quote": name.split("_")[1],
        }

    def parse_futures_name(self, name: str) -> dict:
        return {
            "base": name.split("_")[0],
            "quote": name.split("_")[1],
            "datetime": name.split("_")[2],
        }

    def parse_exchange_info(self, response: dict, parser: dict, **kwargs) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        results = {}
        for data in datas:
            data.update(kwargs)
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results

    def get_id_map(self, exchange_info: dict, market_type: str) -> dict:
        raw_id = {
            "spot": "id",
            "futures": "name",
            "perp": "name",
        }
        infos = self.query_dict(exchange_info, {f"is_{market_type}": True})
        return {v["raw_data"][raw_id[market_type]]: k for k, v in infos.items()}

    def parse_tickers(self, response: dict, exchange_info: dict, market_type: str) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        id_map = self.get_id_map(exchange_info, market_type)
        method_map = {
            "spot": self.parse_spot_ticker,
            "futures": self.parse_futures_ticker,
            "perp": self.parse_perp_ticker,
        }

        key_map = {
            "spot": "currency_pair",
            "futures": "contract",
            "perp": "contract",
        }

        results = {}
        for data in datas:
            instrument_id = id_map[data[key_map[market_type]]]
            results[instrument_id] = method_map[market_type](data, exchange_info[instrument_id])
        return results

    def parse_spot_ticker(self, response: dict, info: dict) -> dict:
        return {
            "symbol": response["currency_pair"],
            "open_time": None,  # Not yet implemented
            "close_time": None,  # Not yet implemented
            "open": None,  # Not yet implemented
            "high": float(response["high_24h"]),
            "low": float(response["low_24h"]),
            "last_price": float(response["last"]),
            "base_volume": float(response["base_volume"]),
            "quote_volume": float(response["quote_volume"]),
            "price_change": float(response["change_utc8"]),
            "price_change_percent": float(response["change_percentage"]) / 100,
            "raw_data": response,
        }

    def parse_futures_ticker(self, response: dict, info: dict) -> dict:
        return {
            "symbol": response["contract"],
            "open_time": None,  # Not yet implemented
            "close_time": None,  # Not yet implemented
            "open": None,  # Not yet implemented
            "high": float(response["high_24h"]),
            "low": float(response["low_24h"]),
            "last_price": float(response["last"]),
            "base_volume": float(response["volume_24h_base"]),
            "quote_volume": float(response["volume_24h_quote"]),
            "price_change": None,  # Not yet implemented
            "price_change_percent": float(response["change_percentage"]) / 100,
            "raw_data": response,
        }

    def parse_perp_ticker(self, response: dict, info: dict) -> dict:
        return {
            "symbol": response["contract"],
            "open_time": None,  # Not yet implemented
            "close_time": None,  # Not yet implemented
            "open": None,  # Not yet implemented
            "high": float(response["high_24h"]),
            "low": float(response["low_24h"]),
            "last_price": float(response["last"]),
            "base_volume": float(response["volume_24h_base"]),
            "quote_volume": float(response["volume_24h_quote"]),
            "price_change": None,  # Not yet implemented
            "price_change_percent": float(response["change_percentage"]) / 100,
            "raw_data": response,
        }

    def get_market_type(self, info: str) -> str:
        if info["is_spot"]:
            return "spot"
        elif info["is_futures"]:
            return "futures"
        elif info["is_perp"]:
            return "perp"
        else:
            raise ValueError(f"Invalid market type. {info}")

    def parse_klines(self, response: dict, market_type: str, info: dict):
        response = self.check_response(response)

        datas = response["data"]
        results = {}
        for data in datas:
            timestamp = self.parse_kline_timestamp(data, market_type)
            results[timestamp] = self.parse_kline(data, market_type, info)
        return results

    def parse_kline_timestamp(self, response: any, market_type: str) -> int:
        if market_type == "spot":
            return int(response[0]) * 1000
        else:
            return response["t"] * 1000

    def parse_kline(self, response: any, market_type: str, info: dict) -> dict:
        if market_type == "spot":
            return {
                "open": float(response[5]),
                "high": float(response[3]),
                "low": float(response[4]),
                "close": float(response[2]),
                "base_volume": float(response[6]),
                "quote_volume": float(response[1]),
                "close_time": None,
                "raw_data": response,
            }
        elif market_type == "futures":
            return {
                "open": float(response["o"]),
                "high": float(response["h"]),
                "low": float(response["l"]),
                "close": float(response["c"]),
                "base_volume": float(response["v"]),  # need to convert into base volume, currently is contract number
                "quote_volume": None,  # not supported
                "close_time": None,
                "raw_data": response,
            }
        else:  # perp
            return {
                "open": float(response["o"]),
                "high": float(response["h"]),
                "low": float(response["l"]),
                "close": float(response["c"]),
                "base_volume": float(response["v"]),  # need to convert into base volume, currently is contract number
                "quote_volume": float(response["sum"]),
                "close_time": None,
                "raw_data": response,
            }

    def get_interval(self, interval: str) -> str:
        if interval not in self.INTERVAL_MAP:
            raise ValueError(f"Invalid interval. {interval}. Must be one of {list(self.INTERVAL_MAP.keys())}")
        return self.INTERVAL_MAP[interval]
