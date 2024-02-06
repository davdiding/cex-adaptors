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

    def parse_exchange_info(self, response: dict, parser: dict):
        response = self.check_response(response)

        results = {}
        datas = response["data"]
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results
