## Developer documentation
### Update package version
1. Update the version in `setup.py`
2. Use `python3 setup.py sdist bdist_wheel` to build the package
3. Use `twine upload dist/*` to upload the package to pypi

### Add new exchange
1. Create new `{exchange_name}.py` file in `cex_adaptors`, `cex_adaptors/exchanges` and `cex_adaptors/parsers` folder.
2. Use class inheritance to implement the new exchange adaptor.
```python
# cex_adaptors/exchanges/{exchange_name}.py
from cex_adaptors.exchanges.{exchange_name} import {ExchangeName}Unified
from cex_adaptors.parsers.{exchange_name} import {ExchangeName}Parser

class {ExchangeName}({ExchangeName}Unified):
    def __init__(self):
        super().__init__()
        self.parser = {ExchangeName}Parser()
```