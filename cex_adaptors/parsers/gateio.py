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
            "contract_size": (lambda x: self.parse_str(x["quanto_multiplier"], float)),
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
            "contract_size": (lambda x: self.parse_str(x["quanto_multiplier"], float)),
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

        key_map = {
            "spot": "currency_pair",
            "futures": "contract",
            "perp": "contract",
        }

        results = {}
        for data in datas:
            instrument_id = id_map[data[key_map[market_type]]]
            results[instrument_id] = self.parse_ticker(data, market_type, exchange_info[instrument_id])
        return results

    def parse_ticker(self, data: dict, market_type: str, info: dict) -> dict:
        return {
            "timestamp": self.get_timestamp(),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": self.get_timestamp(),
            "open": None,
            "high": self.parse_str(data["high_24h"], float),
            "low": self.parse_str(data["low_24h"], float),
            "last": self.parse_str(data["last"], float),
            "base_volume": self.parse_str(data["base_volume" if market_type == "spot" else "volume_24h_base"], float),
            "quote_volume": self.parse_str(
                data["quote_volume" if market_type == "spot" else "volume_24h_quote"], float
            ),
            "price_change": None,
            "price_change_percent": self.parse_str(data["change_percentage"], float) / 100,
            "raw_data": data,
        }

    def get_market_type(self, info: dict) -> str:
        if info["is_spot"]:
            return "spot"
        elif info["is_futures"]:
            return "futures"
        elif info["is_perp"]:
            return "perp"
        else:
            raise ValueError(f"Invalid market type. {info}")

    def get_interval(self, interval: str) -> str:
        if interval not in self.INTERVAL_MAP:
            raise ValueError(f"Invalid interval. {interval}. Must be one of {list(self.INTERVAL_MAP.keys())}")
        return self.INTERVAL_MAP[interval]

    def parse_raw_ticker(self, response: dict, market_type: str, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return self.parse_ticker(data, market_type, info)

    def parse_current_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "timestamp": self.get_timestamp(),
            "next_funding_time": None,  # not yet implemented
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "funding_rate": self.parse_str(data["funding_rate"], float),
            "raw_data": data,
        }

    def parse_history_funding_rate(self, response: dict, info: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        instrument_id = self.parse_unified_id(info)
        market_type = self.parse_unified_market_type(info)

        return [
            {
                "timestamp": self.parse_str(data["t"], int) * 1000,
                "instrument_id": instrument_id,
                "market_type": market_type,
                "funding_rate": self.parse_str(data["r"], float),
                "realized_rate": self.parse_str(data["r"], float),
                "raw_data": data,
            }
            for data in datas
        ]

    def parse_candlesticks(self, response: dict, info: dict, market_type: str, interval: str) -> any:
        response = self.check_response(response)
        datas = response["data"]
        udpate_ = {
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "interval": interval,
        }

        method_map = {
            "spot": self.parse_spot_candlestick,
            "futures": self.parse_futures_candlestick,
            "perp": self.parse_perp_candlestick,
        }

        results = []
        for data in datas:
            result = method_map[market_type](data, info)
            result.update(udpate_)
            results.append(result)
        return results if len(results) > 1 else results[0]

    def parse_spot_candlestick(self, data: list, info: dict) -> dict:
        return {
            "timestamp": self.parse_str(data[0], int) * 1000,
            "open": self.parse_str(data[5], float),
            "high": self.parse_str(data[3], float),
            "low": self.parse_str(data[4], float),
            "close": self.parse_str(data[2], float),
            "base_volume": self.parse_str(data[6], float),
            "quote_volume": self.parse_str(data[1], float),
            "contract_volume": self.parse_str(data[6], float),
            "raw_data": data,
        }

    def parse_perp_candlestick(self, data: dict, info: dict) -> dict:
        return {
            "timestamp": self.parse_str(data["t"], int) * 1000,
            "open": self.parse_str(data["o"], float),
            "high": self.parse_str(data["h"], float),
            "low": self.parse_str(data["l"], float),
            "close": self.parse_str(data["c"], float),
            "base_volume": self.parse_str(data["v"], float) * info["contract_size"],
            "quote_volume": self.parse_str(data["sum"], float),
            "contract_volume": self.parse_str(data["v"], float),
            "raw_data": data,
        }

    def parse_futures_candlestick(self, data: dict, info: dict) -> dict:
        return {
            "timestamp": self.parse_str(data["t"], int) * 1000,
            "open": self.parse_str(data["o"], float),
            "high": self.parse_str(data["h"], float),
            "low": self.parse_str(data["l"], float),
            "close": self.parse_str(data["c"], float),
            "base_volume": self.parse_str(data["v"], float) * info["contract_size"],
            "quote_volume": None,
            "contract_volume": self.parse_str(data["v"], float),
            "raw_data": data,
        }
