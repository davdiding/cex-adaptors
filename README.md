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
| HTX      | 1.0.1   | Yes        | No          |

## Supported API endpoints
| Endpoint            | Exchanges                                         |
|---------------------|---------------------------------------------------|
| `get_exchange_info` | Binance, OKX, Bybit, Gate.io, Kucoin, HTX, Bitget |
| `get_tickers`       | Binance, OKX, Bybit, Gate.io, Kucoin, HTX, Bitget |
| `get_klines`        | Binance, OKX                                      |


## Unified function parameters and output format
### 1. `get_exchange_info`
#### Input
| Parameter     | Required | Default | Description                                  |
|---------------|----------|---------|----------------------------------------------|
| `market_type` | No       | `None`  | should be value in `spot`, `perp`, `futures` |

#### Output (nested dict)
| Field             | Type    | Description                                                                                                                           |
|-------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------|
| `instrument_id`   | `str`   | Key of the `dict`. Format will be `{base}/{quote}:{settle}-{delivery}`                                                                |
| `active`          | `bool`  | Whether this instrument can be traded now                                                                                             |
| `is_spot`         | `bool`  | Whether this instrument is in spot market                                                                                             |
| `is_margin`       | `bool`  | Whether this instrument can trading in margin mode                                                                                    |
| `is_futures`      | `bool`  | Whether this instrument is in futures market                                                                                          |
| `is_perp`         | `bool`  | Whether this instrument is in perp market                                                                                             |
| `is_linear`       | `bool`  | Return `True` when this instrument is settled in stable currency                                                                      |
| `is_inverse`      | `bool`  | Return `True` when this instrument is settled in coin                                                                                 |
| `symbol`          | `str`   | The unified symbol of the trading pair. Format will be `{base}/{quote}`                                                               |
| `base`            | `str`   | The base currency                                                                                                                     |
| `quote`           | `str`   | The quote currency                                                                                                                    |
| `settle`          | `str`   | The settlement currency                                                                                                               |
| `multiplier`      | `int`   | The multiplier. Normaly refer to how many base currency this instrument will include                                                  |
| `leverage`        | `float` | Maximum leverage can be traded.                                                                                                       |
| `listing_time`    | `int`   | Listing time in 13 digits.                                                                                                            |
| `expiration_time` | `int`   | Expiration time in 13 digits.                                                                                                         |
| `contract_size`   | `float` | Contract size. Default is `1`. Refer to how many base currency each contract will include, normaly only apply to `perp` and `futures` |
| `tick_size`       | `float` | The minimum price movement.                                                                                                           |
| `min_order_size`  | `float` | The minimum order size.                                                                                                               |
| `max_order_size`  | `float` | The maximum order size.                                                                                                               |
| `raw_data`        | `dict`  | The unprocessed raw data.                                                                                                             |
