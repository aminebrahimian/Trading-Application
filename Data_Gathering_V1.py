"""Here we are gathering desire candle stick markets from different site API and store them in a Sqlite DB for other procedure use
this version is under test

***Two APIs has been added (Binance and Bybit)

you can find relevant documentes from following links:
https://bybit-exchange.github.io/docs/linear/#t-querykline
https://binance-docs.github.io/apidocs/futures/en/#general-info
"""
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
MaxProcessDuration=0
FirstTimeRun = True
daysbeforenow = 1;PastRecordsFrom = int(time.time())-86340*daysbeforenow #It calculate the exact starting time for data gathering
exchanges = ['bybit','binance']
markets = ['ADAUSDT','ETHUSDT','BTCUSDT','BNBUSDT']
timeframes = [1, 5, 15, 30]    #valid Time frames in minutes 1 3 5 15 30 60 120 240 360 720

#=======================================================Functions=======================================================
def check_connection(exchange):
    if exchange=='bybit': url='https://api.bybit.com'
    elif exchange=='binance': url='https://api.binance.com'

    try:
        requests.get(url, timeout=5)
        Connection = True
    except (requests.ConnectionError, requests.Timeout) as exception:
        winsound.Beep(1000, 100)
        print(datetime.now(), " No internet connection to ", url)
        Connection = False
    return Connection
def get_data(exchange, symbol, timeframe, StartTimeSecs):      #valid Time frames in minutes 1 3 5 15 30 60 120 240 360 720 ## e.g=> get_past_data(exchange="bybit", timeframe="15", symbol="ETHUSDT", StartTimeSecs=1659369391)
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
    table_name = exchange.upper() + '_' + symbol + '_' + str(timeframe)+'m'
    return DataFrame[['open_time','open','high','low','close','volume']], table_name
def checking_deleting_missing_data(dataframe):  #input is a data frame and if some row is missed it will drop the rest of data frame
    starttime = time.time()

    MostCommonValue = (dataframe['open_time'].shift(-1) - dataframe['open_time']).dropna().mode()[0]    #This give the most repeated value time differece
    dataframe['error'] = ((dataframe['open_time'].shift(-1) - dataframe['open_time']).dropna() != MostCommonValue)  #find out is there any missed row
    error_row = (dataframe['open_time'].where(dataframe['error'] == True)).dropna()
    del dataframe['error']
    if len(error_row) > 0:          #if there is any missed rows it will remove the data from that row
        error_row = int(error_row.min())
        dataframe = dataframe[(dataframe['open_time'] <= error_row)]

    stoptime = time.time()
    #print("Process duration: ", stoptime-starttime) #Test point
    return dataframe


def job():
    global FirstTimeRun
    global MaxProcessDuration

    if FirstTimeRun == False:       #getting cyclic row
        starttime = time.time()
        for exc in exchanges:
            for pair in markets:
                for tf in timeframes:  # [5, 30, 60]
                    date = datetime.strptime(str(datetime.now()), "%Y-%m-%d  %H:%M:%S.%f")
                    if ((date.minute % tf) == 0) and (tf <= 60) or ((tf > 60) and (date.minute == 0) and ((date.hour % (tf/60)) == 0)):
                        print(datetime.now())
                        print(exc, " ", pair, " ", tf)
                        df, TableName = get_data(exchange=exc, symbol=pair, timeframe=tf, StartTimeSecs=int(time.time()) - ((tf*60)+55))
                        print(df)
                        print("===============================")


        stoptime = time.time()
        if MaxProcessDuration < (stoptime-starttime): MaxProcessDuration = stoptime-starttime #calculation each cycle process time
        if MaxProcessDuration > 55: print("Error1: each cycle is longer than define and it can cause missing values ")
        print("Maximum process duration: ", MaxProcessDuration) #Test point

    elif FirstTimeRun == True:      #getting passed missed rows
        for exc in exchanges:
            for pair in markets:
                for tf in timeframes:
                    print(exc, " ", pair, " ", tf)
                    df, TableName = get_data(exchange=exc, symbol=pair, timeframe=tf, StartTimeSecs=PastRecordsFrom)
                    print(df)
                    print("===============================")
        FirstTimeRun = False


def live_price():
    pass
#====================================================Main Progress======================================================

job()
schedule.every(1).minute.at(":00").do(job)  # Task Scheduler
while True:
    schedule.run_pending()
    time.sleep(1)