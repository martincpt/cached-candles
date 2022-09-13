import pandas as pd
import numpy as np

from typing import Literal

# Define types
ColumnFilterType = str|list[str]
ColumnRenameType = str|list[str]|dict
        
class CachedDataFrame:
    """
    Handles basic operation of a cached data frame (through a locally stored csv file), 
    such as load, save, append or generating a filtered and renamed version of the 
    cached data frame for the output.

    Args:
        path (str): A path to save / load cached data frames as a CSV file.
        index_col (str | list[str], optional): Index column(s) to set. Defaults to None.
        parse_dates (list[str], optional): Column(s) to parse dates from. Defaults to None.
        column_filter (ColumnFilterType, optional): List of columns to keep in the output data frame. Defaults to None.
        column_rename (ColumnRenameType, optional): List of names to rename columns in the output data frame. Defaults to None.
        init_with (pd.DataFrame, optional): Initial data frame to use (in case cache doesn't exist yet). Defaults to None.
    """

    cache: pd.DataFrame = None

    def __init__(self, 
        path: str, 
        index_col: str|list[str] = None, 
        parse_dates: list[str] = None,
        column_filter: ColumnFilterType = None, 
        column_rename: ColumnRenameType = None,
        init_with: pd.DataFrame = None,
    ) -> None:
        self.path = path
        self.index_col = index_col
        self.parse_dates = parse_dates
        self.column_filter = column_filter
        self.column_rename = column_rename
        # set initial data if needed
        if init_with is not None:
            self.set_cache(init_with)

    def set_cache(self, df: pd.DataFrame|None):
        """Sets the cache data frame.

        Args:
            df (pd.DataFrame): Data frame to set.

        Raises:
            TypeError: If invalid type is passed.
        """        
        if isinstance(df, pd.DataFrame):
            self.cache = CachedDataFrame.setup_df(df, self.index_col, self.parse_dates)
        elif df is None:
            self.cache = None
        else:
            raise TypeError("Cannot set CachedDataFrame.cache because invalid type {} has been given.".format(type(df)))

    def load(self, force = False) -> pd.DataFrame:
        """Loads and returns the data frame from the local cache file, 
        or returns the currently holded data frame if `force` is False.

        Args:
            force (bool, optional): Force to override current data. Defaults to False.

        Returns:
            pd.DataFrame: The cached data frame.
        """        
        if self.cache is None or force:
            # try read file
            try:
                cache = pd.read_csv(self.path, index_col = self.index_col, parse_dates = self.parse_dates)
            except FileNotFoundError:
                cache = None
            # set cache
            self.set_cache(cache)
        # return
        return self.cache

    def save(self) -> None:
        """Saves the current data frame to the local cache file."""
        if isinstance(self.cache, pd.DataFrame):
            self.cache.to_csv(self.path, header=True)
            print("Cache saved")

    def append(self, 
        df: pd.DataFrame, 
        drop_duplicates: list[str] = None, 
        keep: Literal["first", "last", False] = "last",
        save: bool = False,
    ) -> pd.DataFrame:
        """Appends the given data frame to the cache. Drops duplicates if set, by default keeps the last appearance.
        Optionally saves the data frame in to local cache.

        Args:
            df (pd.DataFrame): The data frame to append.
            drop_duplicates (list[str], optional): Subset of columns for identifying duplicates. Defaults to None.
            keep (Literal[&quot;first&quot;, &quot;last&quot;, False], optional): Determines which duplicates (if any) to keep. Defaults to "last".
            save (bool, optional): Saves the updated data frame to the cache if True. Defaults to False.

        Returns:
            pd.DataFrame: The updated data frame.
        """
        # setup df and get a copy of current cache
        df = CachedDataFrame.setup_df(df, self.index_col, self.parse_dates)
        cache = self.cache if self.cache is None else self.cache.copy()
        # make sure we reset the index of both
        for df_part in df, cache:
            if self.index_col and isinstance(df_part, pd.DataFrame):
                df_part.reset_index(inplace = True)
        # concat the new data frame with the cache
        update = pd.concat([cache, df])
        # drop duplicates if needed
        if drop_duplicates:
            update.drop_duplicates(drop_duplicates, keep = keep, inplace = True)
        # replace cache with the update
        self.set_cache(update)
        # save cache 
        if save:
            self.save()
        # return
        return self.cache

    def get_output(self) -> pd.DataFrame:
        """Returns the filtered and renamed output of the cached data frame."""        

        # apply filtering and return
        return CachedDataFrame.apply_filter(self.cache, self.column_filter, self.column_rename)

    def setup_df(df: pd.DataFrame, index_col: str|list[str] = None, parse_dates: list[str] = None) -> pd.DataFrame:
        # make a copy
        df = df.copy()
        # convert list of columns to datetimes
        if isinstance(parse_dates, list|tuple):
            for col in parse_dates:
                if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
                    df[col] = pd.to_datetime(df[col])
        # setup indexing if needed 
        if index_col:
            # set index
            df.set_index(index_col, inplace = True)
            # sort it - just in case
            df.sort_index(inplace = True)
        return df

    def apply_filter(
        df: pd.DataFrame, 
        column_filter: ColumnFilterType = None, 
        column_rename: ColumnRenameType = None
    ) -> None:
        """Class method to filter and rename columns in the given data frame. Mostly this will be used to generate the output.

        Args:
            df (pd.DataFrame): The dateframe to filter and rename.
            column_filter (ColumnFilterType, optional): List of columns to keep in the output data frame. Defaults to None.
            column_rename (ColumnRenameType, optional): List of names to rename columns in the output data frame. Defaults to None.
        """
        # copy the data frame
        df = df.copy()

        # handle default values of filter and rename arguments
        column_filter = [] if column_filter is None else [column_filter] if isinstance(column_filter, str) else column_filter
        column_rename = [] if column_rename is None else [column_rename] if isinstance(column_rename, str) else column_rename

        # do the filtering
        if column_filter:
            # get rid of the rest
            for col in df:
                if col not in column_filter and col in df:
                    del df[col]
        
        # do the rename
        if column_rename:
            if isinstance(column_rename, list):
                df.set_axis(column_rename, axis = 'columns', inplace = True)

            if isinstance(column_rename, dict):
                df.rename(columns = column_rename, inplace = True)

        return df
  