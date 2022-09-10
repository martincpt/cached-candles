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
    api: object = bitfinex.api_v2()
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
        
class CacheLoader:
    """Handles logic to load, save or process the dataframe for the output."""
    cache_path: str = None

    def __init__(self, cache_path: str) -> None:
        self.cache_path = cache_path

    def load(self) -> pd.DataFrame:
        try:
            cache = pd.read_csv(self.cache_path, parse_dates=[TIME_COLUMN])
        except FileNotFoundError:
            cache = None
        return cache

    def save(self, df: pd.DataFrame) -> None:
        df.to_csv(self.cache_path, header=True)

        
class CachedCandles:
    """
    Handles all logic to retrieve candles from cache, or fetches from the given CandlesAPI service
    if it doesn't exist. Puts the results into a dataframe, processes the output, 
    and saves or updates data into the local cache.

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

    def clean_date(date: DateType|ContinousDateType, point: Literal["start", "end"]):
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

    def candles(
        self, symbol: str, interval: str = "1h",
        start: DateType|str = None, end: ContinousDateType|str = None,
        column_filter: ColumnFilterType = None, column_rename: ColumnRenameType = None
    ) -> pd.DataFrame:
        """Fetches and stores candles with cache via CandlesAPI interface.

        Args:
            symbol (str): _description_
            interval (str, optional): _description_. Defaults to "1h".
            start (DateType | str, optional): _description_. Defaults to None.
            end (ContinousDateType | str, optional): _description_. Defaults to None.
            column_filter (ColumnFilterType, optional): _description_. Defaults to None.
            column_rename (ColumnRenameType, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: Time indexed dataframe of requested candles.
        """

        # The list of dateframes we fetched from cache and api calls
        # that we are going to concat at the end.
        dfs = []

        # set `end` to be ContinousType if it's None
        end = end if end is not None else CONTINUOUS

        # helper flag
        is_continous = end in ContinousType.__args__

        # validate and clean dates
        start = CachedCandles.clean_date(start, "start")
        end = CachedCandles.clean_date(end, "end")

        # collect the args
        candle_args = (symbol, interval, start, end)
        
        # get cache path
        cache_path = self.get_cache_path(*candle_args)

        # and check for cache
        try:
            cache = pd.read_csv(cache_path, parse_dates=[TIME_COLUMN])
            print(cache)
        except FileNotFoundError:
            cache = None

        # handle if cache found
        if cache is not None:
            # check whether it's fixed dates or continuous.
            if not is_continous:
                # cache found with fixed dates so we are returning with its finalized format
                # no need for update
                print('Cache found with fixed dates, now we are returning with it.')
                return self.finalize(cache, cache_path, column_filter, column_rename)
            else:
                # cache found but the query itself set to continuous mode
                # we are going to need to check for update
                print('Cache found. Update as continuous.')

                # NOTE:
                # last candle may change overtime so we must drop that and refetch
                # that is because a candle is not getting its final datapoints until the given timeframe ends

                # get the cached df without the last row
                cached_df = cache.drop(cache.tail(1).index)
                # and chain upon the list of dataframes to concat
                dfs.append(cached_df)
                # corrigate the start value for the api call
                start = cache[TIME_COLUMN].max()

        # fetch candles from the api
        result = self.candles_api.candles(*candle_args)

        # convert list of candles to pandas dataframe
        df = pd.DataFrame(result, columns=COLUMNS)

        # convert timestamps to datetime so it has the same format as the cached dataframe
        df[TIME_COLUMN] = pd.to_datetime(df[TIME_COLUMN], unit='ms')

        # append
        dfs.append(df)

        # concat dataframes
        complete_df = pd.concat(dfs)

        # return with the finalized full dataframe  
        return self.finalize(complete_df, cache_path, column_filter, column_rename)

    def apply_filter(
        self, df: pd.DataFrame, 
        column_filter: ColumnFilterType = None, 
        column_rename: ColumnRenameType = None
    ) -> None:
        # handle default values of filter and rename arguments
        column_filter = [] if column_filter is None else [column_filter] if isinstance(column_filter, str) else column_filter
        column_rename = [] if column_rename is None else [column_rename] if isinstance(column_rename, str) else column_rename

        # do the filtering
        if column_filter:
            # get rid of the rest
            for col in COLUMNS:
                if col not in column_filter and col in df:
                    del df[col]
        
        # do the rename
        if column_rename:
            if isinstance(column_rename, list):
                df.set_axis(column_rename, axis='columns', inplace=True)

            if isinstance(column_rename, dict):
                df.rename(column_rename, inplace=True)

    def finalize(
        self, df: pd.DataFrame, cache_path: str,
        column_filter: ColumnFilterType = None, 
        column_rename: ColumnRenameType = None
    ) -> pd.DataFrame:
        """Finalizes the output and stores the unfiltered / unrenamed dataframe in the local cache directory.

        Args:
            df (pd.DataFrame): The dateframe to finalize.
            cache_path (str): The path of the cache file.
            column_filter (list[str], optional): List of columns to keep in the output dataframe. Defaults to None.
            column_rename (list[str], optional): List of names to rename columns in the output dataframe. Defaults to None.

        Returns:
            pd.DataFrame: The finalized and optionally column filtered and renamed dataframe.
        """        
        # setup indexing
        df.set_index(TIME_COLUMN, inplace=True)
        df.sort_index(inplace=True) # just in case

        # save cache 
        df.to_csv(cache_path, header=True)

        print("Cache saved")

        # apply filtering and then return
        self.apply_filter(df, column_filter, column_rename)

        return df

    def get_cache_path(self, *args: datetime.datetime|str) -> str:
        """Returns the path of the cache directory."""
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
    """
    DEFAULT_ARGS = {
        'symbol': config.SYMBOL,
        'interval': config.INTERVAL, 
        'start': config.START_DATE,
        'end': config.END_DATE,
        'limit': 10000, # cannot fetch more than 10000 at a time
        'filter': ['close'], # we only need 'close' value 
        'rename': ['price'], # renamed to 'price'
    }
    df = bitfinex_cache.candles(**DEFAULT_ARGS)
    """
    # bitfinex_cache = BitfinexCachedCandles()
    # bitfinex_cache.candles(**DEFAULT_ARGS)

    # CandlesAPI.get_utc_timestamp(datetime.datetime.utcnow())
    
    """api = BitfinexCandlesAPI()
    cached = CachedCandles(api)

    args = {
        "symbol": "btcusd",
        'interval': "1m", 
        'start': "2022-09-05", # datetime.datetime(2022, 9, 5), # datetime.datetime(2019, 1, 1),
        'end': 'now', # datetime.datetime(2022, 1, 3),
        'limit': 1000, # cannot fetch more than 10000 at a time
        'column_filter': ['close'], # we only need 'close' value 
        'column_rename': ['price'], # renamed to 'price'
    }

    df = cached.candles(**args)

    
    cached.dir_manager.create('test')
    print(api.api_called)"""
    
    # another = BitfinexCandlesAPI()
    # print(another.api_called)

    bitfinex_cache = CachedCandles("bitfinex")

    df = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08", end = "2021-05-15", column_filter = ["close"], column_rename = ["price"])

    print(df)