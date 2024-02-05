# cex_services
This package is designed for interacting with many crypto centralized exchanges. Currently only support **public API endpoints** in Version `1.0.x`. Will add private API endpoints in version `2.0.x`.

## Getting started
use ```pip install cex-services``` to install the package.

## Usage
After installing the package, you can use the following code start the service.
**All the codes is written in async mode.**
```python
from cex_services.binance import Binance
import asyncio
async def main():
    binance = await Binance.create()
    # get exchange info
    print(await binance.get_exchange_info())

    # get all tickers
    print(await binance.get_tickers())

if __name__ == "__main__":
    asyncio.run(main())
```

## Supported Exchanges
| Exchange | Version | Public API | Private API |
|----------|---------|------------|-------------|
| Binance  | 1.0.1   | Yes        | No          |
| OKX      | 1.0.1   | Yes        | No          |
| Bybit    | 1.0.1   | Yes        | No          |
| Gate.io  | 1.0.1   | Yes        | No          |
| Kucoin   | 1.0.1   | Yes        | No          |

## Supported API endpoints
| Endpoint            | Exchanges                            |
|---------------------|--------------------------------------|
| `get_exchange_info` | Binance, OKX, Bybit, Gate.io, Kucoin |
| `get_tickers`       | Binance, OKX                         |
| `get_klines`        | Binance, OKX                         |
