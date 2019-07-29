# Continuous Aggregation in Redis

### Build

`$ cargo build`

### Run

> Only tested in redis 5.x

```shell
$ redis-server --appendonly yes --loadmodule target/debug/libaggregate.dylib
```

```shell
$ redis-cli agg.new btc_usdt time price amount value
OK
$ redis-cli agg.view btc_usdt kline_1m interval 60 first price max price min price last price sum amount sum value
OK
$ redis-cli agg.insert btc_usdt $(($(date +%s)*1000)) 1001.11 1.0 $((1001.11 * 1.0))
OK
$ redis-cli agg.insert btc_usdt $(($(date +%s)*1000)) 1000.01 1.2 $((1000.01 * 1.2))
OK
$ redis-cli agg.save btc_usdt
OK
$ redis-cli hgetall kline_1m
3) "1563715680"
4) "[1001.11,1001.11,1000.01,1000.01,2.2,2201.12]"
```

What the above example does is basically ``select time_bucket('1 minute', time), first(price), max(price), min(price), last(price), sum(amount), sum(value) from btc_usdt group by 1``, but runs in realtime/incremental/streaming way.

### Commands

* ``agg.new key field [field …]``

  Create a stream table with multiple columns, the first field must be time.

* ``agg.view key view_name [INTERVAL seconds] aggfunc field [aggfund field]``

  Create a aggregation view on the stream table, if you specify the optional ``interval`` argument, it's a group by aggregation, like  ``group by time % seconds`` in sql.

  A key named ``view_name`` will be created to store aggregation results, for group by aggretation, the type of key is hash, otherwise, it's a plain string.

* ``agg.insert key time [double …]``

  Insert item into the stream table, will trigger all the aggregations to update. the value of ``time`` is timestamp in milliseconds, with an optional sequence number, seperated with a ``-``, just like the stream entry ID in redis stream. 

  If the sequence number is not provided, it will generate one. 

  If it is provided, the time and sequence pair will be compared with last one, if provided one is equal or smaller than the last one, the operation fails. Can be used to implement idempotence.

  ```
  redis> agg.insert mystream 1564218772000
  1564218772000-0
  redis> agg.insert mystream 1564218772000
  1564218772000-1
  redis> agg.insert mystream 1564218772000-1
  (error) input time is smaller
  redis> agg.insert mystream 1564218772000-2
  1564218772000-2
  redis> agg.insert mystream 1564218772001
  1564218772001-0
  ```

* ``agg.current key ``

  Query current time buckets and the partial aggregation results.

  ```
  redis> agg.current btc_usdt                                                                                                                <<<
  1) kline_1m
  2) 1) "1001.11"
     2) "1001.11"
     3) "1001.11"
     4) "1001.11"
     5) "1"
     6) "1001.11"
  3) "1564391700"
  ```

* ``agg.save key``

  Save current partial aggregation results into standalone key. They will automatically be saved when the time bucket changes in group by aggregation.

* ``agg.last_id key``

  Return the biggest recorded milliseconds-sequence pair.

### Notice

* The first column must be timestamp, the column name is not important, but the position is important.
* The aggregation key only stores partial aggregation state for current time bucket. When save happens, the aggregation results are written into standalone keys.
* Currently supports these aggregation operations:  ``min``, ``max``, ``first``, ``last``, ``sum``, ``avg``, ``count``, ``stds``, ``stdp``, ``vars``, ``varp``.
* Only support ``aof-use-rdb-preamble yes``, or just disable appendonly. It saves me some time to implement aof rewrite operation, i think this option will be the default in the future anyway.

### TODO

* Support more aggregation operations.
* Support month/year group by operation.
