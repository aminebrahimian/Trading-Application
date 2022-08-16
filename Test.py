from pybit import usdt_perpetual
import time

session = usdt_perpetual.HTTP("https://api.bybit.com")
result = session.query_kline(symbol='BTCUSDT', interval=1, limit=1, from_time=int(time.time()-60))['result']
print(result)