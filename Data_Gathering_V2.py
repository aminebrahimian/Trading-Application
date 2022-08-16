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
import itertools
#==================================================Parameters and initilization=========================================
MaxProcessDuration=0
daysbeforenow = 7;PastRecordsFrom = int(time.time())-(86340*daysbeforenow) #It calculate the exact starting time for data gathering
MaxProcessDuration = 0
live_count = 0
#=============Initializing parameters=================
#***Note: Each cycle processing duration could not exceed from the minimum timeframe
exchanges = ['binance', 'bybit']
markets = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
timeframes = [5, 30, 60]    #valid Time frames in minutes 1 3 5 15 30 60 120 240 360 720
DB_Name = "Data_Gathering_DB.db"

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
    try:
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
            DataFrame = DataFrame.astype({"open": float, "high": float, "low": float, "close": float, "volume": float}) #chanching object types to float

    #===============================================Other exchange==========================================================
        elif exchange == 'kucoin':
            print("Kucoin is not implemented yet")

        ProcessStopTime = time.time()
        ProcessTime = ProcessStopTime-ProcessStartTime
        #print("Process time duration", ProcessTime)     #Test point
    except:
        print("Error, Get data exception appear")
    return DataFrame[['open_time','open','high','low','close','volume']]
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
    global MaxProcessDuration
    global PastRecordsFrom
    starttime = time.time()

    for exc in exchanges:
        for pair in markets:
            if not check_connection(exchange=exc): break  # checking API connection and if not it will break the sequence
            date = datetime.strptime(str(datetime.now()), "%Y-%m-%d  %H:%M:%S.%f")
            for tf in timeframes:  # [5, 30, 60]
                if ((date.minute % tf) == 0) and (tf <= 60) or ((tf > 60) and (date.minute == 0) and ((date.hour % (tf/60)) == 0)):
                    #=========DB manipulation==========
                    table_name = exc.upper() + '_' + pair + '_' + str(tf) + 'm'
                    con = sqlite3.connect(DB_Name)
                    Existing_Tables_List = pd.read_sql('SELECT name from sqlite_master where type= \"table\"', con)

                    tables_list = Existing_Tables_List['name'].values.tolist();tables_list.remove('Parameters');tables_list.remove('Live_price')
                    if table_name in tables_list:  #checking existing tables, if exist get the last record and calculate the StartTimeSecs
                        Last_Record = int(pd.read_sql('select open_time from ' + table_name + ' ORDER BY open_time DESC LIMIT 1', con).values[0][0])
                        df = get_data(exchange=exc, symbol=pair, timeframe=tf, StartTimeSecs=Last_Record)
                        df.drop(df.head(1).index, inplace=True)  # drop first row
                    else:                               #getting past records
                        Last_Record = PastRecordsFrom
                        df = get_data(exchange=exc, symbol=pair, timeframe=tf, StartTimeSecs=Last_Record)
                        df.drop(df.tail(1).index, inplace=True)  # drop last row

                    df.to_sql(table_name, con, if_exists='append', index=False)
                    con.close()
                    #==================================
                    print(datetime.now(), "  /  ", time.time(), "  /  ", time.time()-Last_Record)
                    print("Last record", Last_Record)
                    print(exc, " ", pair, " ", tf)
                    print(df)
                    print("===============================")

    stoptime = time.time()
    LastProcessDuration = stoptime-starttime
    print(datetime.now())
    if MaxProcessDuration < LastProcessDuration:MaxProcessDuration = LastProcessDuration #calculation maximum process time
    if MaxProcessDuration > 55: print("Error1: each cycle is longer than define and it can cause missing values ")
    print("Maximum process duration: ", MaxProcessDuration) #Test point

    # load some parameters in DB (these parameters will be used by next module,in order to have an efficient processing)
    df_parameters = pd.DataFrame.from_dict({"update_time": [int(time.time())], "max_process_duration": [round(MaxProcessDuration, 2)],
                                            "last_process_duration": [round(LastProcessDuration, 2)]})
    con = sqlite3.connect(DB_Name)
    df_parameters.to_sql('Parameters', con, if_exists='replace', index=False)
    con.close()
def live_price():
    prices = dict()
    prices['update_time'] = int(time.time())
    for exc in exchanges:
        for symbol in markets:
            try:
                if exc == 'bybit':
                    session = usdt_perpetual.HTTP("https://api.bybit.com")
                    price = session.query_kline(symbol=symbol, interval=1, limit=1, from_time=int(time.time()-60))['result'][0]['close']
                elif exc == 'binance':
                    URL = "https://api.binance.com/api/v3/klines?symbol=" + symbol + "&startTime=" + str(int(time.time()) - 60) + "000&interval=1m&limit=1"
                    price = float(requests.get(url=URL).json()[0][4])
                prices[exc.upper()+"_"+symbol] = price
            except:
                pass

    df_prices = pd.DataFrame([prices])
    con = sqlite3.connect(DB_Name)
    df_prices.to_sql('Live_price', con, if_exists='replace', index=False)
    con.close()
#====================================================Main Progress======================================================

job()
schedule.every(1).minute.at(":00").do(job)  # Task Scheduler (you can change this according to your minimum timeframe)
while True:
    schedule.run_pending()
    time.sleep(1)

    live_count += 1 #This section will call live price every 5 sec
    if live_count == 5:
        live_price()
        live_count = 0