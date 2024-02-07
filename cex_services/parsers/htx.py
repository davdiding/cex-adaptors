from .base import Parser


class HtxParser(Parser):
    def __init__(self):
        super().__init__()

    @staticmethod
    def check_response(response: dict):
        if response["status"] != "ok":
            return {"code": 400, "status": "error", "data": response}
        else:
            return {"code": 200, "status": "success", "data": response["data"]}

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
        response = self.check_response(response)
        if response["code"] != 200:
            return response

        results = {}
        datas = response["data"]
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results
