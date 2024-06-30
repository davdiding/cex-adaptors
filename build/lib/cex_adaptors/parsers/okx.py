from .base import Parser


class OkxParser(Parser):
    interval_map = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
        "2d": "2D",
        "3d": "3D",
        "1w": "1W",
        "1M": "1M",
        "3M": "3M",
    }

    market_type_map = {"spot": "SPOT", "margin": "MARGIN", "futures": "FUTURES", "perp": "SWAP"}
    _market_type_map = {"SPOT": "spot", "MARGIN": "margin", "FUTURES": "futures", "SWAP": "perp"}

    @staticmethod
    def check_response(response: dict):
        if response.get("code") == "0":
            return {"code": 200, "status": "success", "data": response["data"]}
        else:
            raise ValueError(f"Error when parsing OKX response: {response}")

    def _parse_leverage(self, lever: any):
        return int(lever) if lever else 1

    def _parse_symbol(self, data: dict) -> str:
        market_type = data["instType"]
        if market_type in ["SPOT", "MARGIN"]:
            symbol = f"{data['baseCcy']}/{data['quoteCcy']}"
        elif market_type in ["SWAP", "FUTURES"]:
            if data["ctType"] == "linear":
                symbol = f"{data['ctValCcy']}/{data['settleCcy']}"
            else:
                symbol = f"{data['settleCcy']}/{data['ctValCcy']}"
        return symbol

    @property
    def spot_margin_exchange_info_parser(self):
        return {
            "active": (lambda x: x["state"] == "live"),
            "is_spot": (lambda x: self.parse_is_spot(x["instType"])),
            "is_margin": (lambda x: self.parse_is_margin(x["instType"])),
            "is_futures": False,
            "is_perp": False,
            "is_linear": True,
            "is_inverse": False,
            "symbol": (lambda x: self._parse_symbol(x)),
            "base": (lambda x: self.parse_base_currency(x["baseCcy"])),
            "quote": (lambda x: str(x["quoteCcy"])),
            "settle": (lambda x: str(x["quoteCcy"])),
            "multiplier": 1,  # spot and margin default multiplier is 1
            "leverage": (lambda x: self._parse_leverage(x["lever"])),
            "listing_time": (lambda x: int(x["listTime"])),
            "expiration_time": None,  # spot not support this field
            "contract_size": 1,
            "tick_size": (lambda x: float(x["tickSz"])),
            "min_order_size": (lambda x: float(x["minSz"])),
            "max_order_size": (lambda x: float(x["maxMktSz"])),
            "raw_data": (lambda x: x),
        }

    @property
    def futures_perp_exchange_info_parser(self):
        return {
            "active": (lambda x: x["state"] == "live"),
            "is_spot": False,
            "is_margin": False,
            "is_futures": (lambda x: self.parse_is_futures(x["instType"])),
            "is_perp": (lambda x: self.parse_is_perpetual(x["instType"])),
            "is_linear": (lambda x: self.parse_is_linear(x["ctType"])),
            "is_inverse": (lambda x: self.parse_is_inverse(x["ctType"])),
            "symbol": (lambda x: self._parse_symbol(x)),
            "base": (lambda x: self.parse_base_currency(x["ctValCcy"] if x["ctType"] == "linear" else x["settleCcy"])),
            "quote": (lambda x: str(x["settleCcy"] if x["ctType"] == "linear" else x["ctValCcy"])),
            "settle": (lambda x: str(x["settleCcy"])),
            "multiplier": (lambda x: self.parse_multiplier(x["ctMult"])),
            "leverage": (lambda x: self._parse_leverage(x["lever"])),
            "listing_time": (lambda x: int(x["listTime"])),
            "expiration_time": (lambda x: int(x["expTime"]) if x["expTime"] else None),
            "contract_size": (
                lambda x: float(x["ctVal"])
            ),  # linear will be how many base ccy and inverse will be how many quote ccy
            "tick_size": (lambda x: float(x["tickSz"])),
            "min_order_size": (lambda x: float(x["minSz"])),
            "max_order_size": (lambda x: float(x["maxMktSz"])),
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

    def combine_spot_margin_exchange_info(self, spots: dict, margins: dict) -> dict:
        for instrument_id in margins.keys():
            margin = margins[instrument_id]
            if instrument_id in spots.keys() and margin["active"]:
                spot = spots[instrument_id]

                spot["is_margin"] = True
                spot["leverage"] = margin["leverage"]

                if spot["min_order_size"] != margin["min_order_size"]:
                    print(f"min_order_size of {instrument_id} is different between spot and margin")

                if spot["max_order_size"] != margin["max_order_size"]:
                    print(f"max_order_size of {instrument_id} is different between spot and margin")

                if spot["contract_size"] != margin["contract_size"]:
                    print(f"contract_size of {instrument_id} is different between spot and margin")

                if spot["tick_size"] != margin["tick_size"]:
                    print(f"tick_size of {instrument_id} is different between spot and margin")

                spots[instrument_id] = spot
        return spots

    def parse_ticker(self, response: any, market_type: str, info: dict) -> dict:
        if "data" in response:
            response = response["data"][0]

        if market_type == "spot":
            base_volume = float(response["vol24h"])
            quote_volume = float(response["volCcy24h"])
        else:
            base_volume = float(response["volCcy24h"])
            quote_volume = float(response["volCcy24h"]) * (float(response["last"]) + float(response["open24h"])) / 2

        return {
            "timestamp": self.parse_str(response["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "open_time": None,
            "close_time": self.parse_str(response["ts"], int),
            "open": self.parse_str(response["open24h"], float),
            "high": self.parse_str(response["high24h"], float),
            "low": self.parse_str(response["low24h"], float),
            "last": self.parse_str(response["last"], float),
            "base_volume": base_volume,
            "quote_volume": quote_volume,
            "price_change": None,
            "price_change_percent": None,
            "raw_data": response,
        }

    def get_id_map(self, infos: dict, market_type: str = None) -> dict:
        if market_type:
            infos = self.query_dict(infos, {f"is_{market_type}": True})
        return {v["raw_data"]["instId"]: k for k, v in infos.items()}

    def parse_tickers(self, response: dict, market_type: str, infos: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"]

        id_map = self.get_id_map(infos, market_type)
        results = {}
        for data in datas:
            instrument_id = id_map[data["instId"]]
            info = infos[instrument_id]
            results[instrument_id] = self.parse_ticker(data, market_type, info)
        return results

    def parse_funding_rates(self, response: dict, info: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        results = []
        for data in datas:
            results.append(
                {
                    "timestamp": self.parse_str(data["fundingTime"], int),
                    "instrument_id": self.parse_unified_id(info),
                    "market_type": self.parse_unified_market_type(info),
                    "funding_rate": self.parse_str(data["fundingRate"], float),
                    "realized_rate": self.parse_str(data["realizedRate"], float),
                    "raw_data": data,
                }
            )
        return results

    def parse_current_funding_rate(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "timestamp": self.parse_str(data["ts"], int),
            "next_funding_time": self.parse_str(data["nextFundingTime"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self._market_type_map[data["instType"]],
            "funding_rate": self.parse_str(data["fundingRate"], float),
            "raw_data": data,
        }

    def parse_balance(self, response: dict) -> dict:
        response = self.check_response(response)

        datas = response["data"][0]["details"]
        results = {}
        for data in datas:
            currency = data["ccy"]
            results[currency] = {
                "balance": float(data["cashBal"]),
                "available_balance": float(data["availBal"]),
                "raw_data": data,
            }
        return results

    def parse_positions(self, response: dict, infos: dict) -> dict:
        response = self.check_response(response)
        id_map = self.get_id_map(infos)
        datas = response["data"]

        results = {}
        for data in datas:
            instrument_id = id_map[data["instId"]]
            results[instrument_id] = {"position": float(data["pos"]), "raw_data": data}
        return results

    def parse_account_config(self, response: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]

        return {
            "account_id": data["uid"],
            "main_account_id": data["mainUid"],
            "key_name": data["label"],
            "permission": data["perm"].split(","),
            "raw_data": data,
        }

    def get_interval(self, interval: str) -> str:
        if interval not in self.interval_map:
            raise ValueError(f"{interval} is not supported. Must be one of {list(self.interval_map.keys())}")
        return self.interval_map[interval]

    def parse_order_id(self, response: dict) -> str:
        response = self.check_response(response)
        data = response["data"][0]
        return str(data["ordId"])

    def parse_order_info(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "timestamp": int(data["cTime"]),
            "instrument_id": self.parse_unified_id(info),
            "side": data["side"],
            "price": self.parse_str(data["px"], float),
            "volume": self.parse_str(data["sz"], float),
            "fee_ccy": data["feeCcy"],
            "fee": self.parse_str(data["fee"], float),
            "order_id": str(data["ordId"]),
            "order_type": data["ordType"],
            "status": data["state"],
            "raw_data": data,
        }

    def parse_cancel_order(self, response: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "order_id": str(data["ordId"]),
            "raw_data": data,
        }

    def parse_opened_orders(self, response: dict, infos: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        results = []

        id_map = self.get_id_map(infos)

        for data in datas:
            results.append(
                {
                    "timestamp": int(data["cTime"]),
                    "instrument_id": id_map[data["instId"]],
                    "market_type": self._market_type_map[data["instType"]],
                    "side": data["side"],
                    "price": self.parse_str(data["px"], float),
                    "volume": self.parse_str(data["sz"], float),
                    "order_id": str(data["ordId"]),
                    "order_type": data["ordType"],
                    "status": data["state"],
                    "raw_data": data,
                }
            )
        return results

    def parse_history_orders(self, response: dict, infos: dict) -> list:
        response = self.check_response(response)
        datas = response["data"]

        id_map = self.get_id_map(infos)

        results = []
        for data in datas:
            results.append(
                {
                    "timestamp": int(data["fillTime"]),
                    "instrument_id": id_map[data["instId"]],
                    "market_type": self._market_type_map[data["instType"]],
                    "side": data["side"],
                    "executed_price": self.parse_str(data["fillPx"], float),
                    "executed_volume": self.parse_str(data["fillSz"], float),
                    "target_price": self.parse_str(data["px"], float),
                    "target_volume": self.parse_str(data["sz"], float),
                    "fee_currency": data["feeCcy"],
                    "fee": self.parse_str(data["fee"], float),
                    "order_type": data["ordType"],
                    "order_id": str(data["ordId"]),
                    "status": data["state"],
                    "raw_data": data,
                }
            )
        return results

    def parse_last_price(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "timestamp": self.parse_str(data["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "last_price": self.parse_str(data["last"], float),
            "raw_data": data,
        }

    def parse_index_price(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "timestamp": self.parse_str(data["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "index_price": self.parse_str(data["idxPx"], float),
            "raw_data": data,
        }

    def parse_mark_price(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        data = response["data"][0]
        return {
            "timestamp": self.parse_str(data["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "market_type": self.parse_unified_market_type(info),
            "mark_price": self.parse_str(data["markPx"], float),
            "raw_data": data,
        }

    def parse_open_interest(self, response: dict, infos: dict) -> any:
        response = self.check_response(response)
        datas = response["data"]

        id_map = self.get_id_map(infos)

        results = []
        for data in datas:
            results.append(
                {
                    "timestamp": self.parse_str(data["ts"], int),
                    "instrument_id": id_map[data["instId"]],
                    "market_type": self._market_type_map[data["instType"]],
                    "oi_contract": self.parse_str(data["oi"], float),
                    "oi_currency": self.parse_str(data["oiCcy"], float),
                    "raw_data": data,
                }
            )

        return results[0] if len(results) == 1 else results

    def parse_orderbook(self, response: dict, info: dict) -> dict:
        response = self.check_response(response)
        datas = response["data"][0]

        asks = datas["asks"]
        bids = datas["bids"]
        return {
            "timestamp": self.parse_str(datas["ts"], int),
            "instrument_id": self.parse_unified_id(info),
            "asks": [
                {
                    "price": self.parse_str(ask[0], float),
                    "volume": self.parse_str(ask[1], float),
                    "order_number": self.parse_str(ask[3], int),
                }
                for ask in asks
            ],
            "bids": [
                {
                    "price": self.parse_str(bid[0], float),
                    "volume": self.parse_str(bid[1], float),
                    "order_number": self.parse_str(bid[3], int),
                }
                for bid in bids
            ],
            "raw_data": datas,
        }

    def parse_candlesticks(self, response: dict, info: dict, interval: str) -> any:
        def parse_volumes(data: list, market_type: str) -> dict:
            return {
                "base_volume": self.parse_str(data[5 if market_type == "spot" else 6], float),
                "quote_volume": self.parse_str(data[7], float),
                "contract_volume": self.parse_str(data[5], float),
            }

        response = self.check_response(response)
        datas = response["data"]

        instrument_id = self.parse_unified_id(info)
        market_type = self.parse_unified_market_type(info)

        results = []
        for data in datas:
            volumes = parse_volumes(data, market_type)
            results.append(
                {
                    "timestamp": self.parse_str(data[0], int),
                    "instrument_id": instrument_id,
                    "market_type": market_type,
                    "interval": interval,
                    "open": self.parse_str(data[1], float),
                    "high": self.parse_str(data[2], float),
                    "low": self.parse_str(data[3], float),
                    "close": self.parse_str(data[4], float),
                    "base_volume": volumes["base_volume"],
                    "quote_volume": volumes["quote_volume"],
                    "contract_volume": volumes["contract_volume"],
                    "raw_data": data,
                }
            )

        return results if len(results) > 1 else results[0]
