from pybit import usdt_perpetual
import requests

session = usdt_perpetual.HTTP("https://api.bybit.com")
result = session.query_kline(symbol='LUNA2USDT', interval=5, limit=200, from_time=1666301066)['result']
print(result)

URL = "https://api.binance.com/api/v3/klines?symbol=" + 'LUNAUSDT' + "&startTime=" + str(1666301066) + "000&interval=" + '5' + "&limit=1"
result = requests.get(url=URL).json()
print(result)
