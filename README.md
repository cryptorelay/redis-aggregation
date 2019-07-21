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
$ redis-cli agg.insert btc_usdt `date +%s` 1001.11 1.0 `echo '1001.11 * 1.0' | bc`
OK
$ redis-cli agg.insert btc_usdt `date +%s` 1000.01 1.2 `echo '1000.01 * 1.2' | bc`
OK
$ redis-cli agg.save btc_usdt
OK
$ redis-cli hgetall kline_1m
3) "1563715680"
4) "[1001.11,1001.11,1000.01,1000.01,2.2,2201.12]"
```

What the above example does is basically ``select time_bucket('1 minute', time), first(price), max(price), min(price), last(price), sum(amount), sum(value) from btc_usdt group by 1``, but runs in realtime/incremental/streaming way.

### Notice

* The aggregation key only stores partial aggregation state for current time bucket. When save happens, the aggretation results are written into standalone keys.
* The aggregation view with ``interval n`` option is like group by in sql, their results are written into hash value, whose key is timestamp for the time bucket. The aggregation view without ``interval`` option, their results are written into simple string values. 
* Save happens when input time cross time bucket, or command `agg.save` get called.
* The input timestamp need to increase, the inputs with smaller timestamp are ignored.
* Currently supports these aggregation operations:  ``min``, ``max``, ``first``, ``last``, ``sum``, ``avg``, ``count``, ``stds``, ``stdp``, ``vars``, ``varp``.
* Only support ``aof-use-rdb-preamble yes``, or just disable appendonly. It saves me some time to implement aof rewrite operation, i think this option will be the default in the future anyway.

### TODO

* Support more aggregation operations.
* Support month/year group by operation.
* Better support for idempotence.
