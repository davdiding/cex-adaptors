from .base import Parser


class GateioParser(Parser):
    def __init__(self):
        super().__init__()

    @staticmethod
    def check_response(response: dict):
        return {"code": 200, "status": "success", "data": response}

    @property
    def spot_exchange_info_parser(self) -> dict:
        return {}

    def parse_exchange_info(self, response: dict, parser: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        results = {}
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result
        return results
