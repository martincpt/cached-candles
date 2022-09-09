# Cached Crypto Candles for Python (cached_candles)
Fetches and serves crypto candles with local cache storing. Results are returned within a Pandas DataFrame so they are ready to use for data analysis.

This package checks against the local cache, whether the requested candles been already fetched and returns them if they are available. With this, you can save plenty of time and bandwidth, without having to download them over and over again or worry that you get temporary banned by reaching api rate limits.

This package also features a built-in api rate limiter which inserts some sleep time between independent api calls, in case you reach the limit defined in settings. This feature has been tested against the Bitfinex platform.

You don't even have to worry about if the requested timeframe contains more candle than a certain service can provide in a single API call. This library overcomes this issue and splits the query into several API calls and concatenate the results.

## Available platforms (API)
- Bitfinex (relies on akcarsten/bitfinex_api)
- Binance (coming soon)

## Usage
```python
from cached_candles import CachedCandles

bitfinex_cache = CachedCandles("bitfinex")

# this will store a csv file in cache directory 
df = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08", end = "2021-05-15")

# next time you run the same query, it will simply return the local cache result
cached = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08", end = "2021-05-15")

# for simplicty, any change in start or end parameters 
# will result in performing new set of API calls for that timeframe
# even though overlapping data parts may be found in a different cache files
# those will be ignored

# so please note, the following query will fetches the whole timeframe again
# and it stores another cache file, while we just changed the end date to one day later
refetch = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08", end = "2021-05-16")
```

## Continous Cache
One of the coolest thing in this library is **continous** mode, which is default if you haven't defined the ***end*** parameter.

Continous mode can be also used by passing `"now"` literal as the end parameter.
```python
# if end is not defined, continous mode will be used
until_now = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08")

# the same applies for passing 'now'
until_now = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08", end = "now")
```

**Continous mode can pick up where it left off and capable to continously build the cache file without performing unnecessary API calls for existing datapoints.**

```python
# calling the same query 24 hours later, will pick up the changes and / or any new candles only
pickup_changes = bitfinex_cache.candles("btcusd", "1h", start = "2021-05-08", end = "now")
```

## Date types
`start` and `end` parameters can pick up `datetime` types or any parsable datetime string which the `dateutil.parser.parse` lib can parse.

Only `end` can accept `"now"` literal.

## TODO
- Clean cache (refetch)
- Ignore cache