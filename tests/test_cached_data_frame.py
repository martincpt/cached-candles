import unittest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime

from unittest.mock import patch

from cached_candles import CachedDataFrame

class CachedDataFrame_TestCase(unittest.TestCase):

    def setUp(self) -> None:
        pass

if __name__ == '__main__':
    unittest.main()