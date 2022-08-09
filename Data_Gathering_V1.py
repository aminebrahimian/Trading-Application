"""Here we are gathering desire candle stick markets and calculate their Indicators and Oscilators and store them in a Sqlite DB
this version working very well """
# https://bybit-exchange.github.io/docs/linear/#t-querykline    Documentation
# https://binance-docs.github.io/apidocs/futures/en/#general-info
# https://github.com/bukosabino/ta or https://github.com/mrjbq7/ta-lib or https://github.com/twopirllc/pandas-ta or https://github.com/peerchemist/finta  Documentation

from pybit import HTTP
from pybit import usdt_perpetual
import time
import datetime
import schedule
import pandas as pd
import pandas_ta as ta
import sqlite3
from datetime import datetime
import os
import sys
import subprocess
import requests
import winsound
#==================================================Parameters and initilization=========================================
FirstTimeRun = True
daysbeforenow = 10;PastRecordsFrom = int(time.time())-86340*daysbeforenow #It calclate the exact starting time for data gathering
exchanges = ['Bybit','Binance']
markets = ['ADAUSDT','ETHUSDT','BTCUSDT','BNBUSDT']
timeframes = [5, 30, 60]    #valid Time frames in minutes 1 3 5 15 30 60 120 240 360 720

#=======================================================Functions=======================================================
def check_connection(url):
    try:
        requests.get(url, timeout=5)
        Connection = True
    except (requests.ConnectionError, requests.Timeout) as exception:
        winsound.Beep(1000, 100)
        print(datetime.now(), " No internet connection to ", url)
        Connection = False
    return Connection
def get_past_data(exchange, symbol, timeframe, StartTimeSecs):      #valid Time frames in minutes 1 3 5 15 30 60 120 240 360 720 ## e.g=> get_past_data(exchange="bybit", timeframe="15", symbol="ETHUSDT", StartTimeSecs=1659369391)
    ProcessStartTime = time.time()
    DataFrame = pd.DataFrame()
#====================================================bybit==============================================================
    if exchange == 'bybit':
        session = usdt_perpetual.HTTP("https://api.bybit.com")
        TimeDiff = ProcessStartTime - StartTimeSecs         #Calculate number of needed Candles and start Time
        NoOfMissedCandles = int(TimeDiff / (int(timeframe)*60))
        MissedBunchOfCandles = int(NoOfMissedCandles / 200)

        if MissedBunchOfCandles > 0:
            for i in range(MissedBunchOfCandles, 0, -1):
                result = session.query_kline(symbol=symbol, interval=timeframe, limit=200, from_time=StartTimeSecs)['result']
                result = pd.DataFrame.from_dict(result)
                DataFrame = pd.concat([DataFrame, result], ignore_index=True, axis=0)
                StartTimeSecs = StartTimeSecs + (int(timeframe)*60*200)
                NoOfMissedCandles = NoOfMissedCandles - 200
        result = session.query_kline(symbol=symbol, interval=timeframe, limit=NoOfMissedCandles, from_time=StartTimeSecs)['result']
        result = pd.DataFrame.from_dict(result)
        DataFrame = pd.concat([DataFrame, result], ignore_index=True, axis=0)

#===================================================binance=============================================================
    elif exchange == 'binance':
        if int(int(timeframe) / 60) == 0:timeframestr = str(timeframe) + "m"    # translate timeframe formate for binance
        elif int(int(timeframe) / 60) > 0:timeframestr = str(int(int(timeframe) / 60)) + "h"
        ###
        URL = "https://api.binance.com/api/v3/klines?symbol=" + symbol + "&startTime=" + str(StartTimeSecs) + "000&interval=" + timeframestr + "&limit=1"
        result = requests.get(url=URL).json()
        StartTimeSecs = int(result[0][0]/1000)      #Calculate
        ###
        TimeDiff = ProcessStartTime - StartTimeSecs  # Calculate number of needed Candles and start Time
        NoOfMissedCandles = int(TimeDiff / (int(timeframe) * 60))
        MissedBunchOfCandles = int(NoOfMissedCandles / 1000)

        if MissedBunchOfCandles > 0:
            for i in range(MissedBunchOfCandles, 0, -1):
                URL = "https://api.binance.com/api/v3/klines?symbol=" + symbol +"&startTime="+str(StartTimeSecs)+"000&interval=" + timeframestr + "&limit=1000"
                result = requests.get(url=URL).json()
                result = pd.DataFrame(result, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_volume', 'taker_buy_quote_asset_volume', 'ignore'])
                DataFrame = pd.concat([DataFrame, result], ignore_index=True, axis=0)
                StartTimeSecs = StartTimeSecs + (int(timeframe)*60*1000)
                NoOfMissedCandles = NoOfMissedCandles - 1000
        URL = "https://api.binance.com/api/v3/klines?symbol=" + symbol +"&startTime="+str(StartTimeSecs)+"000&interval=" + timeframestr + "&limit="+str(NoOfMissedCandles)
        result = requests.get(url=URL).json()
        result = pd.DataFrame (result, columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_volume', 'taker_buy_quote_asset_volume', 'ignore'])
        DataFrame = pd.concat([DataFrame, result], ignore_index=True, axis=0)
        DataFrame['open_time'] = (DataFrame['open_time'] / 1000).astype(int)

#===============================================Other exchange==========================================================
    elif exchange == 'kucoin':
        print("Kucoin is not implemented yet")

    ProcessStopTime = time.time()
    ProcessTime = ProcessStopTime-ProcessStartTime
    #print("Process time duration", ProcessTime)     #Test point
    table_name = exchange.upper() + '_' + symbol + '_' + timeframe+'m'
    return DataFrame[['open_time','open','high','low','close','volume']], table_name
def checking_deleting_missing_data(dataframe):  #input is a data frame and if some row is missed it will drop the rest of data frame
    starttime = time.time()

    MostCommonValue = (dataframe['open_time'].shift(-1) - dataframe['open_time']).dropna().mode()[0]    #This give the most repeated value time differece
    dataframe['error'] = ((dataframe['open_time'].shift(-1) - dataframe['open_time']).dropna() != MostCommonValue)  #find out is there any missed row
    error_row = (dataframe['open_time'].where(dataframe['error'] == True)).dropna()
    del dataframe['error']
    if len(error_row) > 0:
        error_row = int(error_row.min())
        dataframe = dataframe[(dataframe['open_time'] <= error_row)]

    stoptime = time.time()
    #print("Process duration: ", stoptime-starttime) #Test point
    return dataframe


def job():
    print(datetime.now(), "  ", int(time.time()))
def get_cycle_data(exchanges, symbol):
    pass
def checking_fixing_missing_data(dataframe):
    pass
def first_initilization():
    pass
def live_price():
    pass
#====================================================Main Progress======================================================

job()
schedule.every(1).minute.at(":00").do(job)  # Task Scheduler
while True:
    schedule.run_pending()
    time.sleep(1)