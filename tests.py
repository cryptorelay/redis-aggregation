import pytest
import credis


def test():
    conn = credis.Connection(decode_responses=True)
    conn.execute('del', 'btc_usdt')
    assert conn.execute('agg.new', 'btc_usdt', 'time', 'price', 'amount', 'value') == 'OK'
    assert conn.execute(
        'agg.view', 'btc_usdt', 'kline_1m', 'interval', 60,
        'first', 'price', 'max', 'price', 'min', 'price',
        'last', 'price', 'sum', 'amount', 'sum', 'value') == 'OK'
    assert conn.execute(
        'agg.insert', 'btc_usdt', '1564390729000-0', 1001.11, 1.0, 1001.11 * 1.0) == '1564390729000-0'
    with pytest.raises(credis.base.RedisReplyError):
        conn.execute(
            'agg.insert', 'btc_usdt', '1564390729000-0', 1005.11, 1.0, 1005.11 * 1.0)
    conn.execute(
        'agg.insert', 'btc_usdt', '1564390729000-1', 1005.11, 1.0, 1005.11 * 1.0) == '1564390729000-1'
    assert conn.execute(
        'agg.insert', 'btc_usdt', '1564390731000-0', 999.11, 1.0, 999.11 * 1.0) == '1564390731000-0'
    assert conn.execute(
        'agg.insert', 'btc_usdt', '1564390732000-0', 1003.11, 1.0, 1003.11 * 1.0) == '1564390732000-0'
    name, values, time = conn.execute('agg.current', 'btc_usdt')
    assert name == 'kline_1m'
    assert tuple(values) == ('1001.11', '1005.11', '999.11000000000001', '1003.11', '4', '4008.4400000000001')
    assert time == '1564390680'


if __name__ == '__main__':
    test()
