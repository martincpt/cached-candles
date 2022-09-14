import unittest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import math
import pandas as pd
import datetime

from unittest.mock import patch

from cached_candles import CachedCandles, CandlesAPI, BitfinexCandlesAPI, CONTINUOUS

from test_samples import bitfinex_candle_sample

CLEAN_UP: bool = False

class CachedCandles_TestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.valid_api_name = "bitfinex"
        self.candles_api = BitfinexCandlesAPI()
        self.cached_candles = CachedCandles(self.candles_api, cache_root = __file__)
        self.candles_sample = bitfinex_candle_sample
        self.multiply_api_call_mock = lambda **kwargs: self.candles_sample[
            # [start : end]
            (self.candles_api.api_called - 1) * self.candles_api.limit : self.candles_api.api_called * self.candles_api.limit
        ]
        # default args
        self.start = datetime.datetime.utcfromtimestamp(self.candles_sample[0][0] / 1000)
        self.end = datetime.datetime.utcfromtimestamp(self.candles_sample[-1][0] / 1000 + 60 * 60) # add an extra hour
        self.args = {
            "symbol": "btcusd",
            'interval': "1h", 
            'start': self.start,
            'end': self.end,
        }
    
    def tearDown(self) -> None:
        cache_dir_path = self.cached_candles.cache_dir_path
        cache_api_path = self.cached_candles.cache_api_path
        if CLEAN_UP:
            os.rmdir(cache_api_path)
            os.rmdir(cache_dir_path)

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
        cache_dir_path = os.path.join(os.path.dirname(__file__), custom_cache_dir)
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

    def test_clean_date_continous(self) -> None:
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
        # clean up
        if CLEAN_UP:
            path = self.cached_candles.cached_df.path
            os.remove(path)

    def prepare_multiply_api_call(self, split_by: int) -> None:
        num_of_samples = len(self.candles_sample)
        limit = int(num_of_samples / split_by)
        self.candles_api.limit = limit

    @patch('cached_candles.CandlesAPI.get_utc_now')
    @patch('cached_candles.BitfinexCandlesAPI.api')
    def test_candles_continous_mode(self, bitfinex_mock, get_utc_now_mock):
        print(">>>>>>>>>>>>>>>")
        
        split_by = 2
        self.prepare_multiply_api_call(split_by)
        args = self.args.copy()
        args["end"] = CONTINUOUS
        get_utc_now_mock.return_value = self.end - datetime.timedelta(hours = self.candles_api.limit, seconds = 1)
        bitfinex_mock.candles = self.multiply_api_call_mock
        self.candles_api.api = bitfinex_mock # NOTE: we have to replace the actual object with the mock
        candles = self.cached_candles.candles(**args)
        self.assertEqual(len(candles), self.candles_api.limit)


        print(self.start, self.end, CandlesAPI.get_utc_now(), self.candles_api.limit)
        print(candles)
        print(">>>>>>>>>>>>>>>")
        return
        now = datetime.datetime.utcnow()
        today = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        ytd = today - datetime.timedelta(days = 1)
        b4_ytd = ytd - datetime.timedelta(days = 1)
        hours_passed = math.ceil((now - b4_ytd).total_seconds() / (60 * 60))
        args = ("btcusd", "1h", b4_ytd, "now")
        candles = self.cached_candles.candles(*args)
        print(hours_passed, len(candles))
        
    

if __name__ == '__main__':
    unittest.main()