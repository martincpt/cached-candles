import datetime
import pandas as pd

from typing import Literal
from dateutil.parser import parse, ParserError

from auto_create_directories import AutoCreateDirectories

from cached_candles import CachedDataFrame
from cached_candles import ColumnFilterType, ColumnRenameType

from cached_candles import CandlesAPI, BitfinexCandlesAPI
from cached_candles import ContinousType, DateType, ContinousDateType
from cached_candles import CONTINUOUS, TIME_COLUMN, COLUMNS

# Define constants
CACHE_DIR: str = 'cache'
     
class CachedCandles:
    """
    Provides an interface to access locally cached candles, through a CachedDataFrame instance,
    by symbol, interval, start and end date parameters.
    
    Uses CandlesAPI interface to fetch them if they are not available. Also, it optimizes 
    the forwarded queries to the CandlesAPI instance, so it's capable to pick up 
    where it left off, if the `end = "now"` which is continous mode.

    Handles all logic to create required folder structures, load / save cache from them,
    and to communicate with CandlesAPI instance.

    Responsible to bridge a CandlesAPI and a CachedDataFrame objects,
    and also for creating cache directories.

    Args:
        candles_api (CandlesAPI | str): A name of an available API or a CandlesAPI instance.
        cache_dir (str, optional): Name of the cache directory. Will use `CACHE_DIR: str = 'cache'` if None. Defaults to None.
        cache_root (str, optional): A path to create the cache directory. Will use `__file__` if None. Defaults to None.
    """
    candles_api: CandlesAPI = None
    APIs: tuple[CandlesAPI] = (BitfinexCandlesAPI,)
    cached_df: CachedDataFrame = None
    dir_manager: AutoCreateDirectories = None
    cache_dir_path: str = None
    cache_api_path: str = None
    

    def __init__(self, candles_api: CandlesAPI|str, cache_dir: str = None, cache_root: str = None) -> None:
        # set candles_api
        self.set_candles_api(candles_api)

        # setup directories
        self.set_cache_dir(cache_dir, cache_root)
        
    def set_candles_api(self, candles_api: CandlesAPI|str) -> CandlesAPI:
        """Sets and validates the given CandlesAPI instance or tries to load it from a string.
        by a matching name attribute of CachedCandles.APIs.

        Args:
            candles_api (CandlesAPI | str): A CandlesAPI instance or a string of name identifier.

        Raises:
            ValueError: If invalid name given.
            TypeError: If invalid type given.

        Returns:
            CandlesAPI: The final CandlesAPI instance.
        """        
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

        return self.candles_api
    
    def set_cache_dir(self, cache_dir: str = None, cache_root: str = None) -> str:
        """Creates and sets the cache directory in the provided root directory.

        Args:
            cache_dir (str, optional): Name of the cache directory. Will use `CACHE_DIR: str = 'cache'` if None. Defaults to None.
            cache_root (str, optional): A path to create the cache directory. Will use `__file__` if None. Defaults to None.
        """
        # set defaults
        cache_dir = CACHE_DIR if cache_dir is None else cache_dir
        cache_root = __file__ if cache_root is None else cache_root

        # setup and create required directories
        self.dir_manager = AutoCreateDirectories(base_dir = cache_root)

        # get relative path of cache api directory {cache_dir}/{candles_api.name}
        cache_api = self.dir_manager.join_path(cache_dir, self.candles_api.name)

        # create and store paths
        self.cache_dir_path = self.dir_manager.create(cache_dir)
        self.cache_api_path = self.dir_manager.create(cache_api)

    def clean_date(date: DateType|ContinousDateType|str, point: Literal["start", "end"]) -> DateType|ContinousDateType:
        """Cleans and validates "start" and "end" dates.

        Args:
            date (DateType | ContinousDateType): Datetime object, a parsable datetime string or "now" literal to use continuous mode.
            point (Literal[&quot;start&quot;, &quot;end&quot;]): "start" or "end" literal to distinguishe the two.

        Raises:
            ValueError: If invalid date has been given.

        Returns:
            (DateType | ContinousDateType): The validated or parsed date, or the continuous literal.
        """
        is_end = point == "end"
        is_continous = is_end and date in ContinousType.__args__

        # early return the continuous and datetime types
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
        
        Tries to get candles from local cache first, through a CachedDataFrame instance, 
        and / or fetch and update them from the given a CandlesAPI service. 
        This function bridges the two classes and implement the communication between them.

        Args:
            symbol (str): Symbol of the current market ie.: "btcusd".
            interval (str, optional): Length of the candles. Defaults to "1h".
            start (DateType | str, optional): Start date of requested candles. Can be a datetime object or a datetime parsable string. Defaults to None.
            end (ContinousDateType | str, optional): End date of requested candles. Accepts "now" to use continous mode, or can be a datetime object or a datetime parsable string. Defaults to "now".
            column_filter (ColumnFilterType, optional): List of columns to keep in the output data frame. Defaults to None.
            column_rename (ColumnRenameType, optional): List of names to rename columns in the output data frame. Defaults to None.

        Returns:
            pd.DataFrame: Time indexed data frame of requested candles.
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
        self.cached_df = CachedDataFrame(
            cache_path, 
            index_col = TIME_COLUMN, 
            parse_dates = [TIME_COLUMN], 
            column_filter = column_filter,
            column_rename = column_rename
        )

        # load cache
        cache = self.cached_df.load()

        # handle if cache found
        if cache is not None:
            # check whether it's fixed dates or continuous.
            if not is_continous:
                # cache found with fixed dates so we are returning with its finalized format
                # no need for update
                print('Cache found with fixed dates, now we are returning with it.')
                return self.cached_df.get_output()
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

        # convert list of candles to pandas data frame
        df = pd.DataFrame(result, columns=COLUMNS)

        # convert timestamps to datetime so it has the same format as the cached data frame
        df[TIME_COLUMN] = pd.to_datetime(df[TIME_COLUMN], unit = "ms")
        
        # append to cache
        self.cached_df.append(df, drop_duplicates = [TIME_COLUMN], keep = "last", save = True)

        # return with the finalized full data frame  
        return self.cached_df.get_output()

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
        cache_path = self.dir_manager.join_path(self.cache_api_path, cache_filename)

        return cache_path