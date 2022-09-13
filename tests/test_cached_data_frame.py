import unittest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from cached_candles import CachedDataFrame

CURRENT_DIR: str = os.path.abspath(os.path.dirname(__file__))
TEST_PATH: str = os.path.join(CURRENT_DIR, "test_cached_data_frame.csv")

class CachedDataFrame_TestCase(unittest.TestCase):

    @classmethod
    def setUp(self) -> None:
        self.test_data_init = {
            "date": ["2022-07-12", "2022-07-13", "2022-07-14"],
            "col_1": [1, 2, 3],
            "col_2": [3, 4, 5],
        }
        self.test_data_more = {
            "date": ["2022-07-14", "2022-07-16", "2022-07-17", "2022-07-18"],
            "col_1": [5, 6, 7, 8],
            "col_2": [9, 1, 2, 3],
        }
        self.test_df_init = pd.DataFrame(self.test_data_init)
        self.test_df_more = pd.DataFrame(self.test_data_more)
        self.cached_df = CachedDataFrame(
            path = TEST_PATH, 
            init_with = self.test_df_init,
            index_col = "date",
            parse_dates = ["date"],
        )

    @classmethod
    def tearDownClass(cls):
        os.remove(TEST_PATH)

    def test_set_cache(self) -> None:
        cached_df = CachedDataFrame(path = "doesnt/matter/what/path/now.csv")
        cached_df.set_cache(self.test_df_more)
        self.assertTrue(self.test_df_more.equals(cached_df.cache))
        # test with invalid value as well
        with self.assertRaises(TypeError):
            cached_df.set_cache("Invalid Value")

    def test_load(self) -> None:
        self.test_save() # make sure save ran first - so path exists
        existing_cached_df = CachedDataFrame(path = TEST_PATH)
        existing = existing_cached_df.load()
        self.assertTrue(isinstance(existing, pd.DataFrame))

    def test_load_non_existent(self) -> None:
        non_existent_path = os.path.join(CURRENT_DIR, 'non_existent_path.csv')
        non_existent_cached_df = CachedDataFrame(path = non_existent_path)
        non_existent = non_existent_cached_df.load()
        self.assertIsNone(non_existent)

    def test_save(self) -> None:
        self.cached_df.save()
        self.assertTrue(os.path.exists(TEST_PATH))

    def test_append(self) -> None:
        self.cached_df.append(self.test_df_more)
        total_rows = len(self.test_df_init) + len(self.test_df_more)
        self.assertEqual(len(self.cached_df.cache), total_rows)

    def test_append_to_none(self) -> None:
        self.cached_df.set_cache(None)
        self.cached_df.append(self.test_df_more)
        total_rows = len(self.test_df_more)
        self.assertEqual(len(self.cached_df.cache), total_rows)

    def test_append_drop_duplicates_and_save(self) -> None:
        self.cached_df.append(self.test_df_more, drop_duplicates = ["date"], save = True)
        unique_values = set(list(self.test_df_init["date"].values) + list(self.test_df_more["date"].values))
        self.assertEqual(len(self.cached_df.cache), len(unique_values))
        
    def test_get_output(self) -> None:
        copy = self.cached_df.cache.copy()
        self.cached_df.get_output()
        self.assertTrue(copy.equals(self.cached_df.cache))

    def test_apply_filter(self) -> None:
        df = CachedDataFrame.apply_filter(
            df = self.test_df_init, 
            column_filter = ["col_1"],
            column_rename = ["new_name"],
        )
        self.assertTrue(len(df.columns), 1)
        self.assertTrue("new_name" in df)
        # test rename with dict of map
        df = CachedDataFrame.apply_filter(
            df = self.test_df_init, 
            column_filter = ["col_2"],
            column_rename = {"col_2": "mapped_name"},
        )
        self.assertTrue(len(df.columns), 1)
        self.assertTrue("mapped_name" in df)
        


if __name__ == '__main__':
    unittest.main()