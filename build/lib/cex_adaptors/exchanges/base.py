import json

import aiohttp

from .auth import BinanceAuth, OkxAuth


class BaseClient(object):
    name = None

    def __init__(self) -> None:
        self._session = aiohttp.ClientSession()

    async def _request(self, method: str, url: str, **kwargs):
        if "auth_data" in kwargs:
            # Private endpoint request
            auth_data = kwargs.pop("auth_data")

            # Split into different exchange method
            if self.name == "okx":
                auth = OkxAuth(**auth_data)
                headers = auth.get_private_header(method, url, kwargs.get("params", {}))
                kwargs["headers"] = headers
                if method == "POST":
                    kwargs["data"] = auth.body
                    del kwargs["params"]
            elif self.name == "binance":
                auth = BinanceAuth(**auth_data)
                headers = auth.get_private_header()
                kwargs["params"] = auth.update_params(kwargs.get("params", {}))
                kwargs["headers"] = headers

        if method == "GET":
            async with self._session.get(url, **kwargs) as response:
                return await self._handle_response(response)
        elif method == "POST":
            async with self._session.post(url, **kwargs) as response:
                return await self._handle_response(response)
        else:
            raise ValueError(f"Invalid method: {method}")

    async def _handle_response(self, response: aiohttp.ClientResponse):
        if response.status == 200:
            try:
                return await response.json()
            except Exception as e:
                print(e)
                return json.loads(await response.text())
        else:
            raise Exception(f"Error {response.status} {response.reason} {await response.text()}")

    async def _get(self, url: str, **kwargs):
        return await self._request("GET", url, **kwargs)

    async def _post(self, url: str, **kwargs):
        return await self._request("POST", url, **kwargs)

    async def close(self):
        await self._session.close()
