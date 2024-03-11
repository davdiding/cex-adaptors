from datetime import datetime, timedelta

from ..utils import query_dict


class Parser:
    MULTIPLIER = ["1000000", "100000", "10000", "1000", "100", "10"]
    SPOT_TYPES = ["SPOT"]
    MARGIN_TYPES = ["MARGIN", "both", "utaOnly", "normalOnly"]
    FUTURES_TYPES = [
        "FUTURES",
        "LinearFutures",
        "InverseFutures",
        "NEXT_QUARTER",
        "CURRENT_QUARTER",
        "futures",
        "delivery",
    ]
    PERPETUAL_TYPES = ["SWAP", "LinearPerpetual", "InversePerpetual", "PERPETUAL", "swap", "perpetual"]
    LINEAR_TYPES = ["LinearFutures", "LinearPerpetual", "linear"]
    INVERSE_TYPES = ["InverseFutures", "InversePerpetual", "inverse"]
    STABLE_CURRENCY = ["USDT", "USDC"]
    FIAT_CURRENCY = ["USD"]

    def get_result_with_parser(self, data: dict, parser: dict) -> dict:
        results = {}
        for key in parser:
            if callable(parser[key]):
                results[key] = parser[key](data)
            else:
                results[key] = parser[key]
        return results

    def parse_timestamp_to_str(self, timestamp: int, _format: str = "%y%m%d") -> str:
        return datetime.fromtimestamp(timestamp / 1000).strftime(_format)

    def parse_unified_id(self, info: dict) -> str:
        if info["is_perp"]:
            instrument_id = f"{info['base']}/{info['quote']}:{info['settle']}-PERP"
        elif info["is_futures"]:
            instrument_id = (
                f"{info['base']}/{info['quote']}:{info['settle']}-"
                f"{self.parse_timestamp_to_str(info['expiration_time'])}"
            )
        else:
            multiplier = info["multiplier"] if info["multiplier"] != 1 else ""
            instrument_id = f"{multiplier}{info['base']}/{info['quote']}:{info['settle']}"

        multiplier = info["multiplier"]
        return f"{multiplier if multiplier != 1 and multiplier else ''}{instrument_id}"

    def parse_unified_symbol(self, base: str, quote: str) -> str:
        return f"{base}/{quote}"

    def parse_base_currency(self, base: str) -> str:
        for i in self.MULTIPLIER:
            if i in base:
                return base.replace(i, "")
        return base

    def parse_multiplier(self, base: str) -> int:
        for i in self.MULTIPLIER:
            if i in base:
                return int(i)
        return 1

    def parse_is_futures(self, data: str) -> bool:
        return data in self.FUTURES_TYPES

    def parse_is_perpetual(self, data: str) -> bool:
        return data in self.PERPETUAL_TYPES

    def parse_is_spot(self, data: str) -> bool:
        return data in self.SPOT_TYPES

    def parse_is_margin(self, data: str) -> bool:
        return data in self.MARGIN_TYPES

    def parse_is_linear(self, data: str) -> bool:
        return data in self.LINEAR_TYPES

    def parse_is_inverse(self, data: str) -> bool:
        return data in self.INVERSE_TYPES

    @staticmethod
    def adjust_timestamp(timestamp: int, delta: timedelta) -> int:
        return int((datetime.fromtimestamp(timestamp / 1000) + delta).timestamp() * 1000)

    @staticmethod
    def get_id_symbol_map(info: dict, market_type: str, key: str = "symbol") -> dict:
        if market_type in ["linear", "inverse"]:
            info = query_dict(info, f"is_{market_type} == True and is_spot == False")
        else:
            info = query_dict(info, f"is_{market_type} == True")

        return {v["raw_data"][key]: k for k, v in info.items()}

    @staticmethod
    def parse_str_to_timestamp(_str: str, _format: str = "%Y%m%d") -> int:
        return int(datetime.strptime(_str, _format).timestamp() * 1000)

    @staticmethod
    def query_dict(datas: dict, query: dict) -> dict:
        filtered_data = {}
        for key, value in datas.items():
            if any(value.get(k) == v for k, v in query.items()):
                filtered_data[key] = value
        return filtered_data

    @staticmethod
    def query_dict_by_keys(datas: dict, keys: list) -> dict:
        return {key: datas[key] for key in keys if key in datas}
