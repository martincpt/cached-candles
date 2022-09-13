import unittest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import datetime

from unittest.mock import patch

from cached_candles import CachedCandles, CandlesAPI

class CachedCandles_TestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.valid_api_name = "bitfinex"
        self.cached_candles = CachedCandles(self.valid_api_name, cache_root = __file__)

    def test_init_with_valid_api_name(self):
        cached_candles = CachedCandles(self.valid_api_name)
        self.assertTrue(isinstance(cached_candles.candles_api, CandlesAPI))

    @patch.multiple(CandlesAPI, __abstractmethods__ = set())
    def test_init_with_valid_api_type(self):
        candles_api_mock = CandlesAPI()
        candles_api_mock.name = "Patch name with any string to pass"
        valid_type = candles_api_mock
        cached_candles = CachedCandles(valid_type)
        self.assertTrue(isinstance(cached_candles.candles_api, CandlesAPI))

    def test_init_with_invalid_api_name(self) -> None:
        with self.assertRaises(ValueError):
            CachedCandles("Invalid platform name")

    def test_init_with_invalid_candles_api_type(self) -> None:
        with self.assertRaises(TypeError):
            invalid_type = {"invalid": "object type"}
            CachedCandles(invalid_type)

    def test_init_with_custom_cache_dir(self) -> None:
        custom_cache_dir = "custom"
        cached_candles = CachedCandles(self.valid_api_name, cache_dir = custom_cache_dir, cache_root = __file__)
        cache_dir_base = os.path.join(os.path.dirname(__file__), custom_cache_dir)
        cache_dir_path = os.path.join(cache_dir_base, cached_candles.candles_api.name)
        self.assertEqual(cached_candles.cache_dir_path, cache_dir_path)
        # clean up
        os.rmdir(cache_dir_path)
        os.rmdir(cache_dir_base)

    

if __name__ == '__main__':
    unittest.main()