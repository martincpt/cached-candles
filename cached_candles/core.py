import re
import time
import datetime
import calendar

import pandas as pd
import bitfinex

from abc import ABC, abstractmethod
from typing import Any, Literal, Callable
from collections import defaultdict
from dateutil.parser import parse, ParserError

from auto_create_directories import AutoCreateDirectories

if __name__ == "__main__":
    import add_root_to_sys_path

from cached_candles import CachedDataFrame
from cached_candles.exceptions import APIError

# Define types
ContinousType = Literal["now"]
DateType = datetime.datetime
ContinousDateType = DateType|ContinousType

ColumnFilterType = str|list[str]
ColumnRenameType = str|list[str]|dict

# Bitfinex types
BitfinexCandleLength = Literal["1m", "5m", "15m", "30m", "1h", "3h", "6h", "12h", "1D", "1W"]
BitfinexCandleType = tuple[int, float, float, float, float]
BitfinexCandlesType = list[BitfinexCandleType]

# Define constants
DATASETS_DIR: str = 'datasets'
CONTINUOUS: ContinousType = 'now'

# column names
TIME_COLUMN: str = 'time'
COLUMNS: tuple[str] = (TIME_COLUMN, 'open', 'close', 'high', 'low', 'volume')

class CandlesAPI(ABC):
    """Basic representation of a Candles API service."""

    name: str = None
    api: object = None

    api_called: int = 0

    api_rate_limit_at_every_nth_api_calls: int = 20
    api_rate_limit_sleep_time: int = 60

    CandlesType = list[list]

    def __init__(self) -> None:
        if callable(self.api):
            self.api = self.api()

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
        
class CachedCandles:
    """ TODO: Update docs.
    Handles all logic to retrieve candles from cache, through a CachedDataFrame instance.

    Handles all logic to retrieve candles from cache, or fetches from the given CandlesAPI service
    if it doesn't exist. Puts the results into a dataframe, processes the output, 
    and saves or updates data into the local cache.

    Responsible for calling CandlesAPI service and creating cache directories.

    Args:
        candles_api (CandlesAPI | str): _description_
        cache_root (str, optional): _description_. Defaults to None.
    """
    candles_api: CandlesAPI = None
    APIs: tuple[CandlesAPI] = (BitfinexCandlesAPI,)
    dir_manager: AutoCreateDirectories = None
    cache_dir_path: str = None

    def __init__(self, candles_api: CandlesAPI|str, cache_root: str = None) -> None:
        
        # set candles_api
        self.set_candles_api(candles_api)

        # setup directories
        self.set_cache_dir(DATASETS_DIR, cache_root)
        
    def set_candles_api(self, candles_api: CandlesAPI|str) -> CandlesAPI:
        # load api from string
        if isinstance(candles_api, str):
            # find where api name matches
            api = next((x for x in self.APIs if x.name == candles_api), None)
            # handle invalid parameter or create instance
            if api is None:
                raise ValueError(f"There is no API named `{candles_api}`. Please use valid name parameter.")
            else:
                candles_api = api()

        # break if it's still correct instance
        if not isinstance(candles_api, CandlesAPI):
            raise TypeError("Invalid candles_api parameter. \
                Use either it's name identifier or a CandlesAPI instance.")

        # store candles api
        self.candles_api = candles_api
    
    def set_cache_dir(self, cache_dir: str, cache_root: str = None) -> str:
        # set default cache root
        cache_root = __file__ if cache_root is None else cache_root
        # setup and create required directories
        self.dir_manager = AutoCreateDirectories(base_dir = cache_root)
        cache_dir_path_relative = self.dir_manager.get_path(cache_dir, self.candles_api.name)
        self.cache_dir_path = self.dir_manager.create(cache_dir_path_relative)
        return self.cache_dir_path

    def clean_date(date: DateType|ContinousDateType, point: Literal["start", "end"]) -> datetime.datetime|str:
        """Cleans and validates "start" and "end" dates.

        Args:
            date (DateType | ContinousDateType): Datetime object, a parsable datetime string or "now" literal to use continuous mode.
            point (Literal[&quot;start&quot;, &quot;end&quot;]): "start" or "end" literal to distinguishe the two.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        is_end = point == "end"
        is_continous = is_end and date in ContinousType.__args__

        # earyl return the continuous and datetime types
        if is_continous or isinstance(date, datetime.datetime):
            return date
        
        try:
            return parse(date)
        except (TypeError, ParserError) as e:
            raise ValueError("`{}` must be either a datetime object, a datetime \
                parsable string {} and it is required.".format(point, "or ContinousType" if is_end else ""))

    def candles(self, 
        symbol: str, interval: str = "1h",
        start: DateType|str = None, 
        end: ContinousDateType|str = CONTINUOUS,
        column_filter: ColumnFilterType = None, 
        column_rename: ColumnRenameType = None
    ) -> pd.DataFrame:
        """Fetches and stores candles with cache via CandlesAPI interface.
        
        The main function to get candles through a CachedDataFrame instance, and / or fetch and update them from the given a CandlesAPI service. 
        This function intersects the two classes and implement the communication between them.

        Args:
            symbol (str): Symbol of the current market ie.: "btcusd".
            interval (str, optional): Length of the candles. Defaults to "1h".
            start (DateType | str, optional): Start date of requested candles. Can be a datetime object or a datetime parsable string. Defaults to None.
            end (ContinousDateType | str, optional): End date of requested candles. Accepts "now" to use continous mode, or can be a datetime object or a datetime parsable string. Defaults to "now".
            column_filter (ColumnFilterType, optional): List of columns to keep in the output dataframe. Defaults to None.
            column_rename (ColumnRenameType, optional): List of names to rename columns in the output dataframe. Defaults to None.

        Returns:
            pd.DataFrame: Time indexed dataframe of requested candles.
        """
        # helper flag
        is_continous = end in ContinousType.__args__

        # validate and clean dates
        start = CachedCandles.clean_date(start, "start")
        end = CachedCandles.clean_date(end, "end")

        # get args 
        candle_args = {
            "symbol": symbol,
            "interval": interval,
            "start": start,
            "end": end,
        }

        # get the cache path from arg values
        cache_path = self.get_cache_path(*tuple(candle_args.values()))

        # create a CachedDataFrame instance
        cached_df = CachedDataFrame(
            cache_path, 
            index_col = TIME_COLUMN, 
            parse_dates = [TIME_COLUMN], 
            column_filter = column_filter,
            column_rename = column_rename
        )

        # load cache
        cache = cached_df.load()

        # handle if cache found
        if cache is not None:
            # check whether it's fixed dates or continuous.
            if not is_continous:
                # cache found with fixed dates so we are returning with its finalized format
                # no need for update
                print('Cache found with fixed dates, now we are returning with it.')
                return cached_df.get_output()
            else:
                # cache found but the query itself set to continuous mode
                # we are going to need to check for updates
                print('Cache found. Update as continuous.')

                # NOTE:
                # last candle may change overtime so we must drop that and refetch
                # that's because a candle is not getting its final values until the given timeframe ends

                # corrigate the start value to include update for the api call
                candle_args["start"] = cache.index.max()

        # fetch candles from the api
        result = self.candles_api.candles(**candle_args)

        # convert list of candles to pandas dataframe
        df = pd.DataFrame(result, columns=COLUMNS)

        # convert timestamps to datetime so it has the same format as the cached dataframe
        df[TIME_COLUMN] = pd.to_datetime(df[TIME_COLUMN], unit = "ms")
        
        # append to cache
        cached_df.append(df, drop_duplicates = [TIME_COLUMN], keep = "last", save = True)

        # return with the finalized full dataframe  
        return cached_df.get_output()

    def get_cache_path(self, *args: datetime.datetime|str) -> str:
        """Generates the filename and returns the absolute path of the cache file.

        Args:
            *args (datetime.datetime|str): List of strings or datetime to build the filename from.

        Returns:
            str: Absolute path of the cache file.
        """        
        def format_arg(arg: datetime.datetime|str) -> str:
            arg = arg.isoformat() if isinstance(arg, datetime.datetime) else arg
            return arg.replace(":", "")

        # generate the filename without extension
        cache_filename_no_ext = "-".join(map(format_arg, args))

        # get the filename and path
        cache_filename = f'{cache_filename_no_ext}.csv'
        cache_path = self.dir_manager.get_path(self.cache_dir_path, cache_filename)

        return cache_path

if __name__ == "__main__":
    print("main")
    bitfinex_cache = CachedCandles("bitfinex")
    # df = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08", end = "2021-05-14", column_filter = ["close"], column_rename = ["price"])
    df = bitfinex_cache.candles("btcusd", "1m", start = "2022-09-10 13:30", end = "now")
    # df.to_csv("relative/test09783.csv")
    print(df)