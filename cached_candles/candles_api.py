import re
import time
import datetime
import calendar

import bitfinex

from abc import ABC, abstractmethod
from typing import Any, Literal, Callable
from collections import defaultdict

from cached_candles.exceptions import APIError

# Define types
ContinousType = Literal["now"]
DateType = datetime.datetime
ContinousDateType = DateType|ContinousType

# Bitfinex types
BitfinexCandleLength = Literal["1m", "5m", "15m", "30m", "1h", "3h", "6h", "12h", "1D", "1W"]
BitfinexCandleType = tuple[int, float, float, float, float]
BitfinexCandlesType = list[BitfinexCandleType]

# Define constants
CONTINUOUS: ContinousType = 'now'

# column names
TIME_COLUMN: str = 'time'
COLUMNS: tuple[str] = (TIME_COLUMN, 'open', 'close', 'high', 'low', 'volume')

class CandlesAPI(ABC):
    """Basic representation of a Candles API service."""

    name: str = None
    api: object = None
    api_args: dict = None

    api_called: int = 0

    api_rate_limit_at_every_nth_api_calls: int = 20
    api_rate_limit_sleep_time: int = 60

    CandlesType = list[list]

    def __init__(self) -> None:
        if callable(self.api):
            self.api = self.api() if self.api_args is None else self.api(**self.api_args)

    @abstractmethod
    def candles(
        self, symbol: str, interval: str = "1h",
        start: DateType = None, end: ContinousDateType = None
    ) -> CandlesType:
        """Perform API calls to fetch candles of that particular service.

        Returns:
            CandlesType: List of candles.
        """

    def api_rate_limit_call(self, api_call: Callable) -> Any:
        """Safely excecutes independent api calls by inserting a certain amount of sleep time
            if number of api calls exceeds the limit defined in api rate limit settings.

        Args:
            api_call (Callable): Any callable object this method can call when it's within the limit.
                This can be a direct reference or a lambda callable.

        Returns:
            Any: Returns the result of api_call (Callable).
        """
        # update api called counter
        self.api_called += 1
        # and check if brute force prevention needed
        if self.api_called % self.api_rate_limit_at_every_nth_api_calls == 0:
            sleep_time = self.api_rate_limit_sleep_time
            print(f"-- API rate limit sleep for {sleep_time} sec --")
            time.sleep(sleep_time)
            print("-- Continue --")
        # excecute the api call
        api_result = api_call()
        # and return
        return api_result
    
    def get_utc_now() -> datetime.datetime:
        """Class method to return current UTC datetime."""
        return datetime.datetime.utcnow()

    def get_utc_timestamp(date: datetime.datetime) -> int:
        """Class method to convert a datetime to a UTC timestamp in ms format.

        Args:
            date (datetime.datetime): The datetime object to convert.

        Returns:
            int: UTC timestamp in milliseconds.
        """
        return calendar.timegm(date.timetuple()) * 1000

    def candle_len_2_minutes(interval: str) -> int:
        """Class method to convert candle length to minutes.

        Args:
            interval (str): Candle length like "5m", "6h", "1d", "1w", etc...

        Returns:
            int: Returns the appropriate value in minutes.
        """
        d = {
            'w':      7*24*60,
            'week':   7*24*60,
            'weeks':  7*24*60,
            'd':      24*60,
            'day':    24*60,
            'days':   24*60,
            'h':      60,
            'hr':     60,
            'hour':   60,
            'hours':  60,
        }

        mult_items = defaultdict(lambda: 1).copy()
        mult_items.update(d)

        parts = re.search(r'^(\d+)([^\d]*)', interval.lower().replace(' ', ''))

        if parts:
            return int(parts.group(1)) * mult_items[parts.group(2)] + CandlesAPI.candle_len_2_minutes(re.sub(r'^(\d+)([^\d]*)', '', interval.lower()))
        else:
            return 0

class BitfinexCandlesAPI(CandlesAPI):
    """Candles API service for Bitfinex platform"""

    name: str = "bitfinex"
    api: object = bitfinex.api_v2
    api_args: dict = {"api_key": ""}
    limit: int = 1000

    CandlesType = BitfinexCandlesType

    def candles(
        self, symbol: str, interval: BitfinexCandleLength = "1h", 
        start: DateType = None, end: ContinousDateType = None
    ) -> CandlesType:
        # the list of candles we will return
        candles: BitfinexCandlesType = []

        # helper flags
        is_continous = end in ContinousType.__args__
        is_fixed_date = not is_continous

        # convert `start` and `end` datetimes to timestamps which the api accepts
        # create dictionary with temporary storing the input datetime objects
        timestamps = {"start": start, "end": end}
        # and then convert to timestamps
        for key, date in timestamps.items():
            timestamp = CandlesAPI.get_utc_timestamp(date) if isinstance(date, datetime.datetime) else None
            timestamps[key] = timestamp

        # get candle length in minutes and ms for corrigations
        candle_len_in_minutes = CandlesAPI.candle_len_2_minutes(interval)
        candle_len_in_ms = candle_len_in_minutes * 60 * 1000

        # adjust `end` timestamp
        if is_fixed_date:
            # exclude last candle if it's a fixed date query
            timestamps["end"] = timestamps["end"] - candle_len_in_ms

        if is_continous:
            # use to the last possible candle / data point by getting utcnow as a timestamp
            timestamps["end"] = CandlesAPI.get_utc_timestamp(CandlesAPI.get_utc_now())

        while True:
            # create the api callable object
            api_call = lambda: self.api.candles(
                symbol = symbol, 
                interval = interval, 
                limit = self.limit, 
                start = timestamps["start"], 
                end = timestamps["end"],
                sort = 1
            )

            # get the result by excecuting via api rate limiter
            result = self.api_rate_limit_call(api_call)
        
            # validation
            if len(result) == 0:
                print(f"Empty result, breaking...")
                break

            if result[0] == "error":
                msg = f"Error: {result[1]}, message: f{result[2]}"
                raise APIError(msg, error = result)

            # add result to the growing list
            candles += [tuple(c) for c in result]

            # get last timestamp adjusted with candle length
            last_timestamp = result[-1][0] + candle_len_in_ms

            # check if we reached the end or must perform a new api call
            if last_timestamp < timestamps['end']:
                # start timestamp must be replaced with the corrigated last timestamp
                timestamps['start'] = last_timestamp 
                # convert to utc date so it's printable
                adjusted_start = datetime.datetime.utcfromtimestamp(timestamps["start"] / 1000.0)
                adjusted_end =  datetime.datetime.utcfromtimestamp(timestamps["end"] / 1000.0)
                print(f"Adjusting fetch dates to: {adjusted_start} - {adjusted_end}")
                # and then perform a new api call by stepping to the next iteration
                continue
            else:
                print("Reached the end, breaking...")
            
            break
        
        return candles