from .base import Parser


class GateioParser(Parser):
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
            "leverage": None,
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
