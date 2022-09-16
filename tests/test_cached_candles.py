import unittest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import math
import pandas as pd
import datetime

from unittest.mock import patch

from cached_candles import CachedCandles, CandlesAPI, BitfinexCandlesAPI, CONTINUOUS

from test_candles_api import CandlesAPI_BaseTestCase

CLEAN_UP: bool = True

class CachedCandles_TestCase(CandlesAPI_BaseTestCase):

    api_client_attr_to_mock = "candles"

    def setUp(self) -> None:
        super(CachedCandles_TestCase, self).setUp()
        self.valid_api_name = BitfinexCandlesAPI.name
        self.cached_candles = CachedCandles(self.candles_api, cache_root = __file__)
    
    def tearDown(self) -> None:
        cache_dir_path = self.cached_candles.cache_dir_path
        cache_api_path = self.cached_candles.cache_api_path
        # clean up if empty
        if CLEAN_UP and len(os.listdir(cache_api_path)) == 0:
            os.rmdir(cache_api_path)
            os.rmdir(cache_dir_path)

    def _create_instance(self) -> CandlesAPI:
        return BitfinexCandlesAPI()

    def test_init_with_valid_api_name(self):
        cached_candles = CachedCandles(self.valid_api_name)
        self.assertTrue(isinstance(cached_candles.candles_api, CandlesAPI))

    @patch.multiple(CandlesAPI, __abstractmethods__ = set())
    def test_init_with_valid_api_type(self):
        candles_api_mock = CandlesAPI()
        candles_api_mock.name = "Patch name with any string to pass"
        valid_type = candles_api_mock
        cached_candles = CachedCandles(valid_type, cache_root = __file__)
        self.assertTrue(isinstance(cached_candles.candles_api, CandlesAPI))
        if CLEAN_UP and os.path.exists(cached_candles.cache_api_path):
            os.rmdir(cached_candles.cache_api_path)

    def test_init_with_invalid_api_name(self) -> None:
        with self.assertRaises(ValueError):
            CachedCandles("Invalid platform name")

    def test_init_with_invalid_candles_api_type(self) -> None:
        with self.assertRaises(TypeError):
            invalid_type = {"invalid": "object type"}
            CachedCandles(invalid_type)

    def test_init_with_custom_cache_dir(self) -> None:
        cache_root = self.cached_candles.cache_dir_path
        custom_cache_dir = "custom"
        cached_candles = CachedCandles(self.valid_api_name, cache_dir = custom_cache_dir, cache_root = cache_root)
        cache_dir_path = os.path.join(cache_root, custom_cache_dir)
        cache_api_path = os.path.join(cache_dir_path, cached_candles.candles_api.name)
        self.assertEqual(cached_candles.cache_dir_path, cache_dir_path)
        self.assertEqual(cached_candles.cache_api_path, cache_api_path)
        # clean up
        if CLEAN_UP:
            os.rmdir(cache_api_path)
            os.rmdir(cache_dir_path)

    def test_clean_date(self) -> None:
        date = datetime.datetime(2022, 7, 12)
        cleaned = CachedCandles.clean_date(date, "start")
        self.assertEqual(cleaned, date)

    def test_clean_date_continuous(self) -> None:
        date = CONTINUOUS
        cleaned = CachedCandles.clean_date(date, "end")
        self.assertEqual(cleaned, date)

    def test_clean_date_parse(self) -> None:
        date = datetime.datetime(2022, 7, 12)
        date_string = "2022-07-12"
        cleaned = CachedCandles.clean_date(date_string, "start")
        self.assertEqual(cleaned, date)

    def test_clean_date_invalid_type(self) -> None:
        with self.assertRaises(ValueError):
            CachedCandles.clean_date(None, "start")

    def test_clean_date_unparsable_str(self) -> None:
        with self.assertRaises(ValueError):
            CachedCandles.clean_date("20-20-20", "end")

    def test_get_cache_path(self) -> None:
        date = datetime.datetime(2022, 7, 12)
        args = ("btcusd", "1h", date, "now")
        cache_path = self.cached_candles.get_cache_path(*args)
        cache_filename = os.path.basename(cache_path)
        expected_filename = "btcusd-1h-2022-07-12T000000-now.csv"
        self.assertEqual(cache_filename, expected_filename)

    @patch('cached_candles.BitfinexCandlesAPI.api.candles')
    def test_candles(self, bitfinex_candles_mock) -> None:
        limit = len(self.candles_sample)
        self.candles_api.limit = limit
        bitfinex_candles_mock.return_value = self.candles_sample[:limit]
        result = self.cached_candles.candles(**self.args)
        self.assertEqual(len(result), limit)
        # now a cache exists, check if that returns
        cached = self.cached_candles.candles(**self.args)
        self.assertTrue(cached.equals(result))
        # clean up
        if CLEAN_UP:
            path = self.cached_candles.cached_df.path
            os.remove(path)

    @patch('cached_candles.CandlesAPI.get_utc_now')
    @patch('cached_candles.BitfinexCandlesAPI.api')
    def test_candles_continuous_mode(self, bitfinex_mock, get_utc_now_mock):
        # make a copy of args and set to continuous
        args = self.args.copy()
        args["end"] = CONTINUOUS
        # get cache path
        cache_path = self.cached_candles.get_cache_path(*tuple(args.values()))
        # remove cache if exists
        if os.path.exists(cache_path): os.remove(cache_path)
        # prepare multiply api call
        split_by = 2
        self.prepare_multiply_api_call(split_by, bitfinex_mock)
        # patch
        get_utc_now_mock.return_value = self.end - datetime.timedelta(hours = self.candles_api.limit, seconds = 1)
        # get candles
        candles = self.cached_candles.candles(**args)
        self.assertEqual(len(candles), self.candles_api.limit)
        # patch update
        get_utc_now_mock.return_value = self.end - datetime.timedelta(seconds = 1)
        candles = self.cached_candles.candles(**args)
        self.assertEqual(len(candles), split_by * self.candles_api.limit)
        # clean up
        if CLEAN_UP:
            path = self.cached_candles.cached_df.path
            os.remove(path)
    

if __name__ == '__main__':
    unittest.main()