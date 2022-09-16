import unittest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import random
import datetime

from unittest.mock import patch
from cached_candles import CandlesAPI, BitfinexCandlesAPI, BinanceCandlesAPI
from cached_candles import BitfinexCandleLength, CONTINUOUS
from cached_candles.exceptions import APIError

from binance.exceptions import BinanceAPIException

from test_samples import bitfinex_candle_sample, bitfinex_candle_error_sample, binance_candle_sample

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

class CandlesAPI_BaseTestCase(unittest.TestCase):
    candles_api: CandlesAPI = None
    candles_sample = [(0, 1, 2, 3, 4, 5)]
    api_client_attr_to_mock = None

    def setUp(self) -> None:
        self.candles_api = self._create_instance()
        self.multiply_api_call_mock = lambda **kwargs: self.candles_sample[
            # [start : end]
            (self.candles_api.api_called - 1) * self.candles_api.limit : self.candles_api.api_called * self.candles_api.limit
        ]
        # default args
        self.start = datetime.datetime.utcfromtimestamp(self.candles_sample[0][0] / 1000)
        self.end = datetime.datetime.utcfromtimestamp(self.candles_sample[-1][0] / 1000 + 60 * 60) # add an extra hour
        self.limit = len(self.candles_sample)
        self.args = {
            "symbol": "btcusd",
            'interval': "1h", 
            'start': self.start,
            'end': self.end,
        }

    def _create_instance(self) -> CandlesAPI:
        raise NotImplementedError()

    def prepare_multiply_api_call(self, split_by: int, api_client_mock = None) -> None:
        num_of_samples = len(self.candles_sample)
        limit = int(num_of_samples / split_by)
        self.candles_api.limit = limit
        if api_client_mock is not None:
            # NOTE: we have to replace the actual object with the mock
            setattr(api_client_mock, self.api_client_attr_to_mock, self.multiply_api_call_mock)
            self.candles_api.api = api_client_mock 
    
    def _test_candles_no_result(self, candles_mock) -> None:
        candles_mock.return_value = []
        result = self.candles_api.candles(**self.args)
        self.assertEqual(len(result), 0)

    def _test_candles_error(self) -> None:
        with self.assertRaises(APIError):
            self.candles_api.candles(**self.args)

    def _test_candles_single_api_call(self, candles_mock) -> None:
        limit = len(self.candles_sample)
        self.candles_api.limit = limit
        candles_mock.return_value = self.candles_sample[:limit]
        result = self.candles_api.candles(**self.args)
        self.assertEqual(len(result), limit)
        self.assertEqual(self.candles_api.api_called, 1)

    def _test_candles_multiply_api_call(self, api_client_mock) -> None:
        split_by = 2
        self.prepare_multiply_api_call(split_by, api_client_mock)
        result = self.candles_api.candles(**self.args)
        # self.assertListEqual(result, self.candles_sample)
        self.assertEqual(len(result), len(self.candles_sample))
        self.assertEqual(self.candles_api.api_called, split_by)

    def _test_candles_continuous_mode(self, api_client_mock, get_utc_now_mock):
        split_by = 2
        self.prepare_multiply_api_call(split_by, api_client_mock)
        get_utc_now_mock.return_value = self.end - datetime.timedelta(seconds=1)
        args = self.args.copy()
        args["end"] = CONTINUOUS
        result = self.candles_api.candles(**args)
        # last_date = datetime.datetime.utcfromtimestamp(result[-1][0] / 1000)
        # print(len(result), last_date)
        # self.assertListEqual(result, self.candles_sample)
        self.assertEqual(len(result), len(self.candles_sample))
        self.assertEqual(self.candles_api.api_called, split_by)

class BitfinexCandlesAPI_TestCase(CandlesAPI_BaseTestCase):

    candles_sample = bitfinex_candle_sample
    error_sample = bitfinex_candle_error_sample
    api_client_attr_to_mock = "candles"

    def _create_instance(self) -> CandlesAPI:
        return BitfinexCandlesAPI()
        
    @patch("cached_candles.BitfinexCandlesAPI.api.candles")
    def test_candles_no_result(self, candles_mock) -> None:
        self._test_candles_no_result(candles_mock)

    @patch("cached_candles.BitfinexCandlesAPI.api.candles")
    def test_candles_error(self, candles_mock) -> None:
        candles_mock.return_value = self.error_sample
        self._test_candles_error()

    @patch("cached_candles.BitfinexCandlesAPI.api.candles")
    def test_candles_single_api_call(self, candles_mock) -> None:
        self._test_candles_single_api_call(candles_mock)

    @patch("cached_candles.BitfinexCandlesAPI.api")
    def test_candles_multiply_api_call(self, bitfinex_api_mock) -> None:
        self._test_candles_multiply_api_call(bitfinex_api_mock)

    @patch("cached_candles.CandlesAPI.get_utc_now")
    @patch("cached_candles.BitfinexCandlesAPI.api")
    def test_candles_continuous_mode(self, bitfinex_api_mock, get_utc_now_mock):
        self._test_candles_continuous_mode(bitfinex_api_mock, get_utc_now_mock)

class BinanceCandlesAPI_TestCase(CandlesAPI_BaseTestCase):

    candles_sample = binance_candle_sample
    api_client_attr_to_mock = "get_historical_klines"

    @patch("cached_candles.BinanceCandlesAPI.api.ping", new = lambda x: None)
    def _create_instance(self) -> CandlesAPI:
        return BinanceCandlesAPI()
        
    @patch("cached_candles.BinanceCandlesAPI.api.get_historical_klines")
    def test_candles_no_result(self, candles_mock) -> None:
        self._test_candles_no_result(candles_mock)

    @patch("cached_candles.BinanceCandlesAPI.api")
    def test_candles_error(self, api_client_mock) -> None:
        def raise_error(*args, **kwargs) -> None:
            json_response = '{"code": 123456789, "msg": "Test error"}'
            raise BinanceAPIException(None, 400, json_response)
        setattr(api_client_mock, self.api_client_attr_to_mock, raise_error)
        self.candles_api.api = api_client_mock
        self._test_candles_error()

    @patch("cached_candles.BinanceCandlesAPI.api.get_historical_klines")
    def test_candles_single_api_call(self, candles_mock) -> None:
        self._test_candles_single_api_call(candles_mock)

    @patch("cached_candles.BinanceCandlesAPI.api")
    def test_candles_multiply_api_call(self, binance_api_mock) -> None:
        self._test_candles_multiply_api_call(binance_api_mock)

    @patch('cached_candles.CandlesAPI.get_utc_now')
    @patch("cached_candles.BinanceCandlesAPI.api")
    def test_candles_continuous_mode(self, binance_api_mock, get_utc_now_mock):
        self._test_candles_continuous_mode(binance_api_mock, get_utc_now_mock)
        
if __name__ == '__main__':
    unittest.main()