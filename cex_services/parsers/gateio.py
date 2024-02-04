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

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        results = {}
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results
