import datetime
import pandas as pd

from typing import Literal
from dateutil.parser import parse, ParserError

from auto_create_directories import AutoCreateDirectories

if __name__ == "__main__":
    import add_root_to_sys_path

from cached_candles import CachedDataFrame
from cached_candles import ColumnFilterType, ColumnRenameType

from cached_candles import CandlesAPI, BitfinexCandlesAPI
from cached_candles import ContinousType, DateType, ContinousDateType
from cached_candles import CONTINUOUS, TIME_COLUMN, COLUMNS

# Define constants
DATASETS_DIR: str = 'datasets'
     
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