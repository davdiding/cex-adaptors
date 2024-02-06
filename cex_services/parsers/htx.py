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
        return {}

    def parse_exchange_info(self, response: dict, parser: dict):
        response = self.check_response(response)

        results = {}
        datas = response["data"]
        for data in datas:
            result = self.get_result_with_parser(data, parser)
            id = self.parse_unified_id(result)
            results[id] = result

        return results
