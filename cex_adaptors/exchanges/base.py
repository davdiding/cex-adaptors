import json

import aiohttp

from .auth import OkxAuth


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
                headers = auth.get_private_header(method, url, auth_data.get("params", {}))
                kwargs["headers"] = headers

        async with self._session.request(method, url, **kwargs) as response:
            return await self._handle_response(response)

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
