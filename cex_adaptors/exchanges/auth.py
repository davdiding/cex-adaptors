import base64
import hmac
import json
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

    def get_private_header(self, method: str, request_path: str, params: dict):
        if method == "GET":
            request_path = (request_path + self.parse_params_to_str(params)).replace(self.BASE_ENDPOINT, "")

        body = json.dumps(params) if method == "POST" else ""
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
