import base64
import hashlib
import hmac
import json
import time
from datetime import datetime as dt


class OkxAuth(object):
    BASE_ENDPOINT = "https://www.okx.com"

    def __init__(self, api_key, api_secret, passphrase, use_server_time: bool, debug: bool, flag: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.use_server_time = use_server_time
        self.debug = debug
        self.flag = flag

        self.body = ""

    def get_private_header(self, method: str, request_path: str, params: dict):
        if method == "GET":
            request_path = (request_path + self.parse_params_to_str(params)).replace(self.BASE_ENDPOINT, "")
        elif method == "POST":
            request_path = request_path.replace(self.BASE_ENDPOINT, "")
        else:
            raise ValueError(f"Invalid method: {method}")

        body = json.dumps(params, separators=(",", ":")) if method == "POST" else ""
        self.body = body
        timestamp = self.get_timestamp()

        sign = self.sign(self.pre_hash(timestamp, method, request_path, body))

        header = dict()
        header["Content-Type"] = "application/json"
        header["OK-ACCESS-KEY"] = self.api_key
        header["OK-ACCESS-SIGN"] = sign.decode("utf-8")
        header["OK-ACCESS-TIMESTAMP"] = str(timestamp)
        header["OK-ACCESS-PASSPHRASE"] = self.passphrase
        header["x-simulated-trading"] = self.flag

        if self.debug:
            print("header: ", header)
        return header

    def sign(self, message):
        mac = hmac.new(bytes(self.api_secret, encoding="utf8"), bytes(message, encoding="utf-8"), digestmod="sha256")
        d = mac.digest()
        return base64.b64encode(d)

    def pre_hash(self, timestamp, method, request_path, body):
        if self.debug:
            print("body: ", body)
        return str(timestamp) + str.upper(method) + request_path + body

    @staticmethod
    def get_timestamp():
        now = dt.utcnow()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

    @staticmethod
    def parse_params_to_str(params):
        url = "?"
        for key, value in params.items():
            if value != "":
                url = url + str(key) + "=" + str(value) + "&"
        url = url[0:-1]
        return url


class BinanceAuth(object):
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    def get_private_header(self):
        headers = {"X-MBX-APIKEY": self.api_key}
        return headers

    def update_params(self, params: dict):
        params["timestamp"] = int(time.time() * 1000)

        payload = "&".join([f"{k}={v}" for k, v in params.items()])
        signature = hmac.new(self.api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
        params["signature"] = signature
        return params


class BybitAuth(object):
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

        self.payload = ""

    def get_private_header(self, method: str, request_path: str, params: dict):
        timestamp = self.get_timestamp()
        recv_window = 5000
        payload = self.get_payload(method, params)
        self.payload = payload

        signature = self.get_signature(payload, timestamp, recv_window)

        headers = {
            "Content-Type": "application/json",
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-RECV-WINDOW": str(recv_window),
        }

        return headers

    @staticmethod
    def get_timestamp():
        return int(time.time() * 1000)

    @staticmethod
    def get_payload(method: str, params: dict):
        def cast_values():
            string_params = [
                "qty",
                "price",
                "triggerPrice",
                "takeProfit",
                "stopLoss",
            ]
            integer_params = ["positionIdx"]
            for key, value in params.items():
                if key in string_params:
                    if type(value) != str:
                        params[key] = str(value)
                elif key in integer_params:
                    if type(value) != int:
                        params[key] = int(value)

        if method == "GET":
            payload = "&".join([f"{k}={v}" for k, v in sorted(params.items()) if v is not None])
            return payload
        else:
            cast_values()
            return json.dumps(params)

    def get_signature(self, payload: str, timestamp: int, recv_window: int):
        param_str = str(timestamp) + self.api_key + str(recv_window) + payload

        hash_str = hmac.new(
            bytes(self.api_secret, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        )
        return hash_str.hexdigest()
