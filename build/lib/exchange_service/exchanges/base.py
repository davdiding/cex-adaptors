import aiohttp


class BaseClient(object):
    def __init__(self) -> None:
        self._session = aiohttp.ClientSession()

    async def _request(self, method: str, url: str, **kwargs):
        return await self._handle_response(await self._session.request(method, url, **kwargs))

    async def _handle_response(self, response: aiohttp.ClientResponse):
        if response.status == 200:
            return await response.json()
        else:
            raise Exception(f"Error {response.status} {response.reason} {await response.text()}")

    async def _get(self, url: str, **kwargs):
        return await self._request("GET", url, **kwargs)

    async def _post(self, url: str, **kwargs):
        return await self._request("POST", url, **kwargs)

    async def close(self):
        await self._session.close()
