from .base import BaseClient


class Bitmex(BaseClient):
    name = "bitmex"

    def __init__(self):
        super().__init__()
