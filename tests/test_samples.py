import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime

from cached_candles import BitfinexCandlesAPI

# btcusd 1h 2022-07-12 - 2022-07-16
bitfinex_candle_sample = [
    (1657584000000, 19945, 19806, 19980, 19779, 134.82641599), 
    (1657587600000, 19794, 19867.42775783, 19933.64039017, 19785, 108.34244875), 
    (1657591200000, 19867, 19954, 19968, 19865.97253515, 38.54362335), 
    (1657594800000, 19950, 19917, 19950.37545109, 19867, 234.45159659), 
    (1657598400000, 19917, 19992, 20011, 19908, 1197.35561038), 
    (1657602000000, 19985, 19941, 20038.23365274, 19941, 33.36169658), 
    (1657605600000, 19941, 19830.52800898, 19941, 19788, 145.96621968), 
    (1657609200000, 19830, 19725.95257602, 19849, 19611, 375.62544074), 
    (1657612800000, 19725.95257602, 19761.3762208, 19802, 19660, 187.39576299), 
    (1657616400000, 19761, 19575.15120552, 19785, 19543.89885799, 149.56014169), 
    (1657620000000, 19578, 19584, 19695, 19576, 133.47719749), 
    (1657623600000, 19592, 19762.76137214, 19803, 19581, 765.30571643), 
    (1657627200000, 19758, 19853, 19942, 19758, 182.61113906), 
    (1657630800000, 19855, 19748.64712728, 19981, 19731, 294.38171899), 
    (1657634400000, 19748, 19892, 19904.059654, 19670, 274.53156908), 
    (1657638000000, 19892, 19848.56760589, 19960, 19824, 595.54489157), 
    (1657641600000, 19847.56540607, 19877.63140093, 19970, 19846.56320624, 97.3810279),
    (1657645200000, 19873, 19844.55880658, 19892, 19792, 44.73380152), 
    (1657648800000, 19844.55880658, 19704, 19851, 19680.19803469, 69.39424005), 
    (1657652400000, 19704.25083057, 19370, 19704.25083057, 19262, 950.12077216), 
    (1657656000000, 19368, 19430.29482658, 19528, 19357.48968987, 50.3902887), 
    (1657659600000, 19430.29482658, 19326, 19498, 19323, 57.10117867), 
    (1657663200000, 19325, 19351, 19449.2929149, 19216.56633301, 500.27136907), 
    (1657666800000, 19339, 19310.53347612, 19423.17054346, 19235, 157.35328628)
]

bitfinex_candle_error_sample = ['error', 123456, 'Sample error message.']

class BitfinexCandlesAPI_TestUtil:
    def setUp(self) -> None:
        self.candles_api = BitfinexCandlesAPI()
        self.candles_sample = bitfinex_candle_sample
        self.error_sample = bitfinex_candle_error_sample
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

    def prepare_multiply_api_call(self, split_by: int, mock = None) -> None:
        num_of_samples = len(self.candles_sample)
        limit = int(num_of_samples / split_by)
        self.candles_api.limit = limit
        if mock is not None:
            # NOTE: we have to replace the actual object with the mock
            mock.candles = self.multiply_api_call_mock
            self.candles_api.api = mock 