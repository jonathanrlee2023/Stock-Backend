import os
from typing import List

import orjson


class TickerInfo:
    def __init__(self, timestamp):
        self._high = None
        self._low = None
        self._volume = None
        self._volatility = None
        self._last = None
        self._ask = None
        self._bid = None
        self._symbol = None
        self._timestamp = timestamp

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        self._timestamp = timestamp

    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, symbol):
        self._symbol = symbol

    @property
    def bid(self):
        return self._bid

    @bid.setter
    def bid(self, bid):
        self._bid = bid

    @property
    def ask(self):
        return self._ask

    @ask.setter
    def ask(self, ask):
        self._ask = ask

    @property
    def last(self):
        return self._last

    @last.setter
    def last(self, last):
        self._last = last

    @property
    def high(self):
        return self._high

    @high.setter
    def high(self, high):
        self._high = high

    @property
    def low(self):
        return self._low

    @low.setter
    def low(self, low):
        self._low = low

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, volume):
        self._volume = volume

    @property
    def volatility(self):
        return self._volatility

    @volatility.setter
    def volatility(self, volatility):
        self._volatility = volatility

    def __str__(self):
        return f"{self._symbol} at {self._timestamp}: last price: {self.last}"

    def append_to_file(self):
        # check to see if file exists. If it doesn't, don't try to read existing contents
        filename = self._symbol + ".json"
        if os.path.exists(filename):
            with open(filename, "r") as file:
                file_content = file.read()
                data = orjson.loads(file_content, List[TickerInfo])
            data.append(self)
        else:
            data = [self]

        with open(filename, "w") as file:
            json = orjson.dumps(data, jdkwargs={'indent': 4}, strip_privates=True)
            file.write(json)