from .exchanges.biget import BitgetUnified
from .parsers.biget import BitgetParser


class Bitget(object):
    def __init__(self) -> None:
        self.bitget = BitgetUnified()
        self.parser = BitgetParser()
        self.exchange_info = {}
