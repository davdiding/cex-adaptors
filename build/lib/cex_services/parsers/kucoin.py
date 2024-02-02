from .base import Parser


class KucoinParser(Parser):
    def check_response(self, response: dict):
        if response.get("code") == "200000":
            return response.get("data")
        else:
            raise Exception(f"Kucoin API Error: {response}")

    @property
    def spot_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["enableTrading"]),
            "is_spot": True,
            "is_margin": (lambda x: x["isMarginEnabled"]),
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self.parse_unified_symbol(x["baseCurrency"], x["quoteCurrency"])),
            "base": (lambda x: self.parse_base_currency(x["baseCurrency"])),
            "quote": (lambda x: str(x["quoteCurrency"])),
            "settle": (lambda x: str(x["quoteCurrency"])),
            "multiplier": 1,  # spot multiplier default 1
            "leverage": 1,  # spot leverage default 1
            "listing_time": None,  # api not support this field
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,  # spot contract size default 1
            "tick_size": None,  # Not yet implemented
            "min_order_size": None,  # Not yet implemented
            "max_order_size": None,  # Not yet implemented
            "raw_data": (lambda x: x),
        }

    @property
    def futures_exchange_info_parser(self) -> dict:
        return {
            "active": (lambda x: x["status"] == "Open"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: True if x["expireDate"] else False),
            "is_perp": (lambda x: False if x["expireDate"] else True),
            "is_linear": (lambda x: True),  # Not yet implemented
            "is_inverse": (lambda x: False),  # Not yet implemented
            "symbol": (lambda x: self.parse_unified_symbol(x["baseCurrency"], x["quoteCurrency"])),
            "base": (lambda x: self.parse_base_currency(x["baseCurrency"])),
            "quote": (lambda x: str(x["quoteCurrency"])),
            "settle": (lambda x: str(x["quoteCurrency"])),
            "multiplier": (lambda x: abs(int(x["multiplier"]))),
            "leverage": (lambda x: x["maxLeverage"]),
            "listing_time": (lambda x: x["firstOpenDate"] if x["firstOpenDate"] else None),
            "expiration_time": (lambda x: x["expireDate"] if x["expireDate"] else None),
            "contract_size": 1,  # Not yet implemented
            "tick_size": None,  # Not yet implemented
            "min_order_size": None,  # Not yet implemented
            "max_order_size": None,  # Not yet implemented
            "raw_data": (lambda x: x),
        }

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        datas = self.check_response(response)

        results = {}
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results
