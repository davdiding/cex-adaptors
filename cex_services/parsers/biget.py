from .base import Parser


class BitgetParser(Parser):
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
