import unittest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import random
import datetime

from unittest.mock import patch
from cached_candles import CandlesAPI, BitfinexCandlesAPI, BitfinexCandleLength, CONTINUOUS
from cached_candles.exceptions import APIError

from test_samples import bitfinex_candle_sample, bitfinex_candle_error_sample
from test_samples import BitfinexCandlesAPI_TestUtil

class CandlesAPI_TestCase(unittest.TestCase):

    @patch.multiple(CandlesAPI, __abstractmethods__ = set())
    def test_api_rate_limit_call(self) -> None:
        # setup
        candles_api = CandlesAPI()
        return_value = [[0, 1, 2, 3, 4, 5]]
        api_call = lambda: return_value

        # test default behaviour
        result = candles_api.api_rate_limit_call(api_call)
        self.assertListEqual(result, return_value)
        
        # test rate limiter with 0 sleep time
        candles_api.api_rate_limit_at_every_nth_api_calls = 1
        candles_api.api_rate_limit_sleep_time = 0
        result_with_sleep = candles_api.api_rate_limit_call(api_call)
        self.assertListEqual(result_with_sleep, return_value)

    def test_get_utc_now(self) -> None:
        old_now = datetime.datetime.utcnow()
        new_now = CandlesAPI.get_utc_now()
        self.assertGreaterEqual(new_now, old_now)

    def test_get_utc_timestamp(self) -> None:
        # simple date
        date = datetime.datetime(2000, 5, 8)
        timestamp = CandlesAPI.get_utc_timestamp(date)
        self.assertEqual(timestamp, 957744000_000)
        # go marty !!!
        date = datetime.datetime(1955, 11, 12, 22, 4)
        timestamp = CandlesAPI.get_utc_timestamp(date)
        self.assertEqual(timestamp, -446090160_000)

    def test_candle_len_2_minutes(self) -> None:
        # generate random samples and test
        multiply = {"m": 1, "h": 60, "d": 24 * 60, "w": 24 * 60 * 7}
        samples = BitfinexCandleLength.__args__
        for sample in samples:
            unit = sample[-1].lower()
            rand = random.randint(0, 59)
            rand_candle_len = f"{rand}{unit}"
            rand_in_minutes = rand * multiply[unit]
            result = CandlesAPI.candle_len_2_minutes(rand_candle_len)
            self.assertEqual(result, rand_in_minutes)

class BitfinexCandlesAPI_TestCase(BitfinexCandlesAPI_TestUtil, unittest.TestCase):
        
    @patch('cached_candles.BitfinexCandlesAPI.api.candles')
    def test_candles_no_result(self, bitfinex_candles_mock) -> None:
        bitfinex_candles_mock.return_value = []
        result = self.candles_api.candles(**self.args)
        self.assertEqual(len(result), 0)

    @patch('cached_candles.BitfinexCandlesAPI.api.candles')
    def test_candles_error(self, bitfinex_candles_mock) -> None:
        bitfinex_candles_mock.return_value = self.error_sample
        with self.assertRaises(APIError):
            self.candles_api.candles(**self.args)

    @patch('cached_candles.BitfinexCandlesAPI.api.candles')
    def test_candles_single_api_call(self, bitfinex_candles_mock) -> None:
        limit = len(self.candles_sample)
        self.candles_api.limit = limit
        bitfinex_candles_mock.return_value = self.candles_sample[:limit]
        result = self.candles_api.candles(**self.args)
        self.assertEqual(len(result), limit)
        self.assertEqual(self.candles_api.api_called, 1)

    @patch('cached_candles.BitfinexCandlesAPI.api')
    def test_candles_multiply_api_call(self, bitfinex_mock) -> None:
        split_by = 2
        self.prepare_multiply_api_call(split_by, bitfinex_mock)
        result = self.candles_api.candles(**self.args)
        self.assertListEqual(result, self.candles_sample)
        self.assertEqual(self.candles_api.api_called, split_by)

    @patch('cached_candles.CandlesAPI.get_utc_now')
    @patch('cached_candles.BitfinexCandlesAPI.api')
    def test_candles_continuous_mode(self, bitfinex_mock, get_utc_now_mock):
        split_by = 2
        self.prepare_multiply_api_call(split_by, bitfinex_mock)
        get_utc_now_mock.return_value = self.end - datetime.timedelta(seconds=1)
        args = self.args.copy()
        args["end"] = CONTINUOUS
        result = self.candles_api.candles(**args)
        # last_date = datetime.datetime.utcfromtimestamp(result[-1][0] / 1000)
        # print(len(result), last_date)
        self.assertListEqual(result, self.candles_sample)
        self.assertEqual(self.candles_api.api_called, split_by)
        
if __name__ == '__main__':
    unittest.main()