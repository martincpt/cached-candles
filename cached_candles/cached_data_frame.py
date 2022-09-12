import pandas as pd

from typing import Literal

# Define types
ColumnFilterType = str|list[str]
ColumnRenameType = str|list[str]|dict
        
class CachedDataFrame:
    """
    Handles basic operation of a cached data frame, such as load, save, append
    or process the cached data frame for the output.

    Args:
        path (str): Cache path.
        index_col (str | list[str], optional): Index column(s) to set. Defaults to None.
        parse_dates (list, optional): Column(s) to parse dates from. Defaults to None.
        column_filter (ColumnFilterType, optional): List of columns to keep in the output data frame. Defaults to None.
        column_rename (ColumnRenameType, optional): List of names to rename columns in the output data frame. Defaults to None.
    """

    cache: pd.DataFrame = None

    def __init__(self, 
        path: str, 
        index_col: str|list[str] = None, 
        parse_dates: list = None,
        column_filter: ColumnFilterType = None, 
        column_rename: ColumnRenameType = None,
    ) -> None:
        
        self.path = path
        self.index_col = index_col
        self.parse_dates = parse_dates
        self.column_filter = column_filter
        self.column_rename = column_rename

    def load(self) -> pd.DataFrame:
        """Loads the data frame from the local cache file.

        Returns:
            pd.DataFrame: The cached data frame.
        """
        try:
            self.cache = pd.read_csv(self.path, index_col = self.index_col, parse_dates = self.parse_dates)
        except FileNotFoundError:
            self.cache = None
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
        # copy new data
        df = df.copy()
        # get default value or a copy of cache df or a copy without index
        cache = self.cache if self.cache is None else self.cache.copy() if self.index_col is None else self.cache.reset_index()
        # concat the new data frame with the cache
        update = pd.concat([cache, df])
        # drop duplicates if needed
        if drop_duplicates:
            update.drop_duplicates(drop_duplicates, keep = keep, inplace = True)
        # setup indexing
        update.set_index(self.index_col, inplace = True)
        # sort index - just in case
        update.sort_index(inplace = True)
        # replace cache with the update
        self.cache = update
        # save cache 
        if save:
            self.save()
        # return
        return self.cache

    def get_output(self) -> pd.DataFrame:
        """Returns the filtered and renamed output of the cached data frame."""        

        # apply filtering and return
        return CachedDataFrame.apply_filter(self.cache, self.column_filter, self.column_rename)

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
                df.set_axis(column_rename, axis='columns', inplace=True)

            if isinstance(column_rename, dict):
                df.rename(column_rename, inplace=True)

        return df
  