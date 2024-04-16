from ..utils import query_dict
from .base import Parser


class HtxParser(Parser):

    response_keys = ["data", "ticks"]

    INTERVAL_MAP = {
        "spot": {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "60min",
            "4h": "4hour",
            "1d": "1day",
            "1M": "1mon",
            "1w": "1week",
            "1y": "1year",
        },
        "linear": {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "60min",
            "4h": "4hour",
            "1d": "1day",
            "1M": "1mon",
        },
        "inverse_perp": {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "60min",
            "4h": "4hour",
            "1d": "1day",
            "1M": "1mon",
        },
        "inverse_futures": {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            "1h": "60min",
            "4h": "4hour",
            "1d": "1day",
            "1M": "1mon",
        },
    }

    def __init__(self):
        super().__init__()

    def check_htx_response(self, response: dict):
        if response["status"] == "ok":
            for _key in self.response_keys:
                if _key in response:
                    results = {"code": 200, "status": "success", "data": response[_key]}
                    results.update({"timestamp": response["ts"]} if "ts" in response else {})
                    return results
        else:
            raise ValueError(f"Error when parsing HTX response: {response}")

    @property
    def spot_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["state"] == "online"),
            "is_spot": True,
            "is_margin": False,
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(x["bcdn"], x["qcdn"])),
            "base": (lambda x: self.parse_base_currency(x["bcdn"])),
            "quote": (lambda x: str(x["qcdn"])),
            "settle": (lambda x: str(x["qcdn"])),
            "multiplier": 1,  # spot and margin default multiplier is 1
            "leverage": (lambda x: float(x["lr"]) if x["lr"] else 1),  # spot and margin default leverage is 1
            "listing_time": (lambda x: int(x["toa"])),
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,
            "tick_size": None,  # not yet implemented
            "min_order_size": None,  # not yet implemented
            "max_order_size": None,  # not yet implemented
            "raw_data": (lambda x: x),
        }

    @property
    def linear_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["contract_status"] == 1),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: self.parse_is_futures(x["business_type"])),
            "is_perp": (lambda x: self.parse_is_perpetual(x["business_type"])),
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(self.parse_pair(x)["base"], self.parse_pair(x)["quote"])),
            "base": (lambda x: self.parse_base_currency(self.parse_pair(x)["base"])),
            "quote": (lambda x: self.parse_pair(x)["quote"]),
            "settle": (lambda x: self.parse_pair(x)["quote"]),
            "multiplier": 1,
            "leverage": None,  # not yet implemented
            "listing_time": (lambda x: self.parse_str_to_timestamp(x["create_date"]) if x["create_date"] else None),
            "expiration_time": (
                lambda x: self.parse_str_to_timestamp(x["delivery_date"]) if x["delivery_date"] else None
            ),
            "contract_size": (lambda x: float(x["contract_size"])),
            "tick_size": (lambda x: float(x["price_tick"])),
            "min_order_size": None,  # not yet implemented
            "max_order_size": None,  # not yet implemented
            "raw_data": (lambda x: x),
        }

    @property
    def inverse_futures_exchange_info_parser(self):
        return {
            "active": (lambda x: x["contract_status"] == 1),
            "is_spot": False,
            "is_margin": False,
            "is_futures": True,
            "is_perp": False,
            "is_linear": False,
            "is_inverse": True,
            "symbol": (lambda x: self.parse_unified_symbol(x["symbol"], "USD")),
            "base": (lambda x: str(x["symbol"])),
            "quote": "USD",
            "settle": (lambda x: str(x["symbol"])),
            "multiplier": 1,
            "leverage": None,  # not yet implemented
            "listing_time": (lambda x: self.parse_str_to_timestamp(x["create_date"]) if x["create_date"] else None),
            "expiration_time": (lambda x: int(x["delivery_time"]) if x["delivery_time"] else None),
            "contract_size": (lambda x: float(x["contract_size"])),
            "tick_size": (lambda x: float(x["price_tick"])),
            "min_order_size": None,  # not yet implemented
            "max_order_size": None,  # not yet implemented
            "raw_data": (lambda x: x),
        }

    @property
    def inverse_perp_exchange_info_parser(self):
        return {
            "active": (lambda x: x["contract_status"] == 1),
            "is_spot": False,
            "is_margin": False,
            "is_futures": False,
            "is_perp": True,
            "is_linear": False,
            "is_inverse": True,
            "symbol": (lambda x: self.parse_unified_symbol(x["symbol"], "USD")),
            "base": (lambda x: str(x["symbol"])),
            "quote": "USD",
            "settle": (lambda x: str(x["symbol"])),
            "multiplier": 1,
            "leverage": None,  # not yet implemented
            "listing_time": (lambda x: self.parse_str_to_timestamp(x["create_date"]) if x["create_date"] else None),
            "expiration_time": None,
            "contract_size": (lambda x: float(x["contract_size"])),
            "tick_size": (lambda x: float(x["price_tick"])),
            "min_order_size": None,  # not yet implemented
            "max_order_size": None,  # not yet implemented
            "raw_data": (lambda x: x),
        }

    @staticmethod
    def parse_pair(response: dict) -> dict:
        if response["delivery_date"]:
            return {
                "base": response["pair"].split("-")[0],
                "quote": response["pair"].split("-")[1],
                "datetime": response["delivery_date"],
            }
        else:
            return {
                "base": response["pair"].split("-")[0],
                "quote": response["pair"].split("-")[1],
            }

    def parse_exchange_info(self, response: dict, parser: dict):
        response = self.check_htx_response(response)

        results = {}
        datas = response["data"]
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results

    def get_htx_id_map(self, exchange_info: dict, market_type: str) -> dict:
        keys_map = {
            "spot": "sc",
            "linear": "contract_code",
            "inverse_perp": "contract_code",
            "inverse_futures": "symbol",
        }
        if market_type == "linear":
            infos = query_dict(exchange_info, "(is_perp == True or is_futures == True) and is_linear == True")
        elif market_type == "inverse_perp":
            infos = query_dict(exchange_info, "is_perp == True and is_inverse == True")
        elif market_type == "inverse_futures":
            infos = query_dict(exchange_info, "is_futures == True and is_inverse == True")
            return {self.parse_inverse_futures_symbol(v["raw_data"]): k for k, v in infos.items()}
        else:
            infos = self.query_dict(exchange_info, {f"is_{market_type}": True})
        return {v["raw_data"][keys_map[market_type]]: k for k, v in infos.items()}

    def parse_inverse_futures_symbol(self, datas: dict) -> str:
        contract_type_map = {
            "this_week": "CW",
            "next_week": "NW",
            "quarter": "CQ",
        }
        return f"{datas['symbol']}_{contract_type_map[datas['contract_type']]}"

    def parse_tickers(self, response: dict, exchange_infos: dict, market_type: str) -> dict:
        response = self.check_htx_response(response)

        method_map = {
            "spot": self.parse_spot_ticker,
            "linear": self.parse_linear_ticker,
            "inverse_perp": self.parse_inverse_perp_ticker,
            "inverse_futures": self.parse_inverse_futures_ticker,
        }

        keys_map = {
            "spot": "symbol",
            "linear": "contract_code",
            "inverse_perp": "contract_code",
            "inverse_futures": "symbol",
        }

        id_map = self.get_htx_id_map(exchange_infos, market_type)

        results = {}
        datas = response["data"]
        for data in datas:
            if data[keys_map[market_type]] not in id_map:
                print(f"Unmapped symbol: {data[keys_map[market_type]]} in {market_type}")
                continue
            instrument_id = id_map[data[keys_map[market_type]]]
            params = {
                "response": data,
                "info": exchange_infos[instrument_id],
            }
            if market_type == "spot":
                params["timestamp"] = response["timestamp"]
            results[instrument_id] = method_map[market_type](**params)
        return results

    def parse_ticker(self, response: dict, market_type: str, info: dict) -> dict:
        method_map = {
            "spot": self.parse_spot_ticker,
            "linear": self.parse_linear_ticker,
            "inverse_perp": self.parse_inverse_perp_ticker,
            "inverse_futures": self.parse_inverse_futures_ticker,
        }
        params = {
            "response": response,
            "info": info,
        }
        if market_type == "spot":
            params["timestamp"] = response["ts"]
        return method_map[market_type](**params)

    def parse_spot_ticker(self, response: dict, info: dict, timestamp: str):
        return {
            "timestamp": self.parse_str(timestamp, int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": None,
            "open": self.parse_str(response["open"], float),
            "high": self.parse_str(response["high"], float),
            "low": self.parse_str(response["low"], float),
            "last": self.parse_str(response["close"], float),
            "base_volume": self.parse_str(response["amount"], float),
            "quote_volume": self.parse_str(response["vol"], float),
            "price_change": self.parse_str(response["close"], float) - self.parse_str(response["open"], float),
            "price_change_percent": None,
            "raw_data": response,
        }

    def parse_linear_ticker(self, response: dict, info: dict):
        return {
            "timestamp": self.parse_str(response["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": self.parse_str(response["ts"], int),
            "open": self.parse_str(response["open"], float),
            "high": self.parse_str(response["high"], float),
            "low": self.parse_str(response["low"], float),
            "last": self.parse_str(response["close"], float),
            "base_volume": self.parse_str(response["amount"], float) * info["contract_size"],
            "quote_volume": self.parse_str(response["vol"], float),
            "price_change": self.parse_str(response["close"], float) - self.parse_str(response["open"], float),
            "price_change_percent": None,
            "raw_data": response,
        }

    def parse_inverse_perp_ticker(self, response: dict, info: dict):
        return {
            "timestamp": self.parse_str(response["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": self.parse_str(response["ts"], int),
            "open": self.parse_str(response["open"], float),
            "high": self.parse_str(response["high"], float),
            "low": self.parse_str(response["low"], float),
            "last": self.parse_str(response["close"], float),
            "base_volume": self.parse_str(response["amount"], float),
            "quote_volume": self.parse_str(response["vol"], float),
            "price_change": self.parse_str(response["close"], float) - self.parse_str(response["open"], float),
            "price_change_percent": None,
            "raw_data": response,
        }

    def parse_inverse_futures_ticker(self, response: dict, info: dict):
        return {
            "timestamp": self.parse_str(response["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": self.parse_str(response["ts"], int),
            "open": self.parse_str(response["open"], float),
            "high": self.parse_str(response["high"], float),
            "low": self.parse_str(response["low"], float),
            "last": self.parse_str(response["close"], float),
            "base_volume": self.parse_str(response["amount"], float),
            "quote_volume": self.parse_str(response["vol"], float),
            "price_change": self.parse_str(response["close"], float) - self.parse_str(response["open"], float),
            "price_change_percent": None,
            "raw_data": response,
        }

    def get_market_type(self, info: dict) -> str:
        if info["is_spot"]:
            return "spot"
        elif info["is_linear"] and (info["is_futures"] or info["is_perp"]):
            return "linear"
        elif info["is_inverse"] and info["is_futures"]:
            return "inverse_futures"
        elif info["is_inverse"] and info["is_perp"]:
            return "inverse_perp"
        else:
            raise ValueError("Unknown market type")

    def get_interval(self, interval: str, market_type: str) -> str:
        if interval not in self.INTERVAL_MAP[market_type]:
            raise ValueError(
                f"Invalid interval: {interval} in {market_type}, must be one of {list(self.INTERVAL_MAP[market_type])}"
            )
        return self.INTERVAL_MAP[market_type][interval]

    def parse_current_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_htx_response(response)
        data = response["data"]
        return {
            "timestamp": response["timestamp"],
            "next_funding_time": self.parse_str(data["funding_time"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "funding_rate": self.parse_str(data["funding_rate"], float),
            "raw_data": data,
        }

    def parse_history_funding_rate(self, response: dict, info: dict) -> list:
        response = self.check_htx_response(response)
        datas = response["data"]["data"]

        instrument_id = self.parse_unified_id(info)
        market_type = self.parse_unified_market_type(info)

        return [
            {
                "timestamp": self.parse_str(data["funding_time"], int),
                "instrument_id": instrument_id,
                "market_type": market_type,
                "funding_rate": self.parse_str(data["funding_rate"], float),
                "realized_rate": self.parse_str(data["realized_rate"], float),
                "raw_data": data,
            }
            for data in datas
        ]

    def parse_candlesticks(self, response: dict, info: dict, market_type: str, interval: str) -> any:
        """
        - spot :
        [
            {
                'id': 1713110400,
                'open': 64186.51,
                'close': 66184.98,
                'low': 62578.94,
                'high': 66566.0,
                'amount': 1187.8846778540208,
                'vol': 77002399.88436274,
                'count': 86506
            }
        ]

        - linear :
        {
            "amount":0.004
            "close":13076.8
            "count":1
            "high":13076.8
            "id":1603695060
            "low":13076.8
            "open":13076.8
            "trade_turnover":52.3072
            "vol":4
        }

        - inverse:
        {
            "id":1628652420
            "open":45875.02
            "close":45851
            "low":45850.93
            "high":45880.01
            "amount":2.6471133153472475
            "vol":1214
            "count":50
        }

        """

        response = self.check_htx_response(response)
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

        return results if len(results) > 1 else results[0]

    def parse_candlestick(self, data: dict, info: dict, market_type: str) -> dict:

        return {
            "timestamp": self.parse_str(data["id"], int) * 1000,
            "open": self.parse_str(data["open"], float),
            "high": self.parse_str(data["high"], float),
            "low": self.parse_str(data["low"], float),
            "close": self.parse_str(data["close"], float),
            "base_volume": self.parse_str(data["amount"], float),
            "quote_volume": (
                self.parse_str(data["vol" if market_type != "linear" else "trade_turnover"], float)
                * (1 if info["is_linear"] else info["contract_size"])
            ),
            "contract_volume": self.parse_str(data["amount" if market_type == "spot" else "vol"], float),
            "raw_data": data,
        }
