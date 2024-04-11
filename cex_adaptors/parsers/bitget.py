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
        if response["code"] == "00000":
            return {
                "code": 200,
                "status": "success",
                "data": response["data"],
                "timestamp": self.parse_str(response["requestTime"], int),
            }
        else:
            raise ValueError(f"Error in parsing Bitget response: {response}")

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

    def parse_ticker(self, response: dict, info: dict, market_type: str):
        return {
            "timestamp": self.parse_str(response["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,  # API not support
            "close_time": self.parse_str(response["ts"], int),
            "open": self.parse_str(response["open" if market_type == "spot" else "open24h"], float),
            "high": self.parse_str(response["high24h"], float),
            "low": self.parse_str(response["low24h"], float),
            "last": self.parse_str(response["lastPr"], float),
            "base_volume": self.parse_str(response["baseVolume"], float),
            "quote_volume": self.parse_str(response["quoteVolume"], float),
            "price_change": None,  # API not support
            "price_change_percent": self.parse_str(response["change24h"], float),
            "raw_data": response,
        }

    def parse_raw_ticker(self, response: dict, info: dict, market_type: str):
        response = self.check_response(response)
        data = response["data"][0]
        return self.parse_ticker(data, info, market_type)

    def parse_tickers(self, response: dict, exchange_info: dict, market_type: str) -> dict:
        response = self.check_response(response)

        datas = response["data"]

        id_map = self.get_bitget_id_map(exchange_info, market_type)
        results = {}
        for data in datas:
            if data["symbol"] not in id_map:
                print(f"Unmapped symbol: {data['symbol']} in {market_type} in Bitget")
                continue
            instrument_id = id_map[data["symbol"]]
            results[instrument_id] = self.parse_ticker(data, exchange_info[instrument_id], market_type)
        return results

    def parse_mark_index_price(self, response: dict, info: dict, query_type: str) -> dict:
        response = self.check_response(response)
        data = response["data"][0]

        return {
            "timestamp": self.parse_str(data["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "index_price": self.parse_str(data["indexPrice" if query_type == "index" else "markPrice"], float),
            "raw_data": data,
        }

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

    def parse_current_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]

        return {
            "timestamp": response["timestamp"],
            "next_funding_time": None,
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "funding_rate": self.parse_str(data["fundingRate"], float),
            "raw_data": data,
        }

    def parse_history_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        return [
            {
                "timestamp": self.parse_str(data["fundingTime"], int),
                "instrument_id": self.parse_unified_id(info),
                "market": self.parse_unified_market_type(info),
                "funding_rate": self.parse_str(data["fundingRate"], float),
                "realized_rate": self.parse_str(data["fundingRate"], float),
                "raw_data": data,
            }
            for data in datas
        ]

    def parse_orderbook(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        return {
            "timestamp": self.parse_str(datas["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "asks": [
                {
                    "price": self.parse_str(ask[0], float),
                    "volume": self.parse_str(ask[1], float),
                    "order_number": None,
                }
                for ask in datas["asks"]
            ],
            "bids": [
                {
                    "price": self.parse_str(bid[0], float),
                    "volume": self.parse_str(bid[1], float),
                    "order_number": None,
                }
                for bid in datas["bids"]
            ],
            "raw_data": datas,
        }
