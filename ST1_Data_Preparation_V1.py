"""
Here we are gathering desire data from Data_Gathering_DB.db and prepare it for the next module that is
"""
import pandas as pd
import sqlite3
from pybit import usdt_perpetual
import time
from datetime import datetime
import schedule
import numpy as np
from scipy.signal import argrelextrema
from sklearn.linear_model import LinearRegression
import winsound

global markets
global FirstShortConditionLatch
global FirstLongConditionLatch
global SecondShortConditionLatch
global SecondLongConditionLatch
global timeframes

exchanges = ['BYBIT', 'BINANCE']
markets = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
timeframes = [5, 60]
FirstShortConditionLatch = [False, False, False]
FirstLongConditionLatch = [False, False, False]
SecondShortConditionLatch = [False, False, False]
SecondLongConditionLatch = [False, False, False]

FirstTimeRun = True
Job_Delay = 20      #Delay parameter for starting procedure in seconds
Working_Status = "Normal"   #this can be chosen for whole procedure "Backtest" or "Normal"
#===========Functions handlling section============
def job():
    time.sleep(Job_Delay)
    start_time = datetime.now()
    for exc in exchanges:
        for Symbol in markets:
            for tf in timeframes:
                Data=data_gathering_preparing(Exchange=exc, Symbol=Symbol, timeframe=tf, DBname="Data_Gathering_DB.db", TailNumber=200)    #Fetching data from DB and preparing for modules
                Data=swing_detector_prepertion(DataFrame=Data, Order=6)     #Adding swings to the Data farme
                Data=swing_detector_prepertion_order2(DataFrame=Data, Order=2)
                #Data=linear_regression_support_Resistance(DataFrame=Data, SwingCount=4, SwingThreshold=0)     #Adding Linear regression support and resistance to the Data farme
                #Data=sub_linear_regression_support_Resistance(DataFrame=Data, SwingCount=3, SwingThreshold=0)
                #Data=polynomial_regression_support_Resistance(DataFrame=Data, SwingCount=4, SwingThreshold=0, degree=2)
                #Data=sub_polynomial_regression_support_Resistance(DataFrame=Data, SwingCount=4, SwingThreshold=0, degree=2)
                #Data=trend_power_calculation(DataFrame=Data)

                #======DB Manipulation==============
                TableName = exc+'_'+Symbol+'_'+str(tf)
                con = sqlite3.connect("Strategy1_DB.db")
                Data.to_sql(name=TableName, con=con, if_exists='replace')
                #Data_Temp = {'open_time': [Data['open_time'][-1]], 'time_tag': [Data.index[-1]], 'Res_Coefficient': [Data['Res_Coefficient'][-1]], 'Sup_Coefficient': [Data['Sup_Coefficient'][-1]]}  # Create Tables
                #df_Temp = pd.DataFrame(Data_Temp)
                #df_Temp.to_sql('Market_Logs_'+Symbol, con, if_exists='append', index=False)
                #con.close()
                #print(Data)

    stop_time = datetime.now()
    print("Start time: ", start_time, "   Stop time: ", stop_time, "   Duration: ", stop_time-start_time)

#===========Data gathering and calculation============
def data_gathering_preparing(Exchange, Symbol, timeframe, DBname, TailNumber):
    con = sqlite3.connect(DBname)
    df = pd.read_sql('SELECT open_time,open,high,low,close,volume FROM ' + Exchange + '_' + Symbol + '_' + str(timeframe) + 'm', con).tail(TailNumber)
    con.close()
    df['open_time'] = df['open_time'] - 14400  # adjustin time zone
    df['Time_Tag'] = pd.to_datetime(df['open_time'], unit='s')  # changing time format from seconds to time and date
    df = df.set_index("Time_Tag")
    return df
def swing_detector_prepertion(DataFrame, Order):         #this function get some data from DB and find outs its swing high and low
    df = DataFrame
    # Calculating and preparing Swing dataframe swing will detected after 6 * 5 min candles
    df['swing_high'] = df['high'][(df['high'].shift(6) <= df['high']) & (df['high'].shift(5) <= df['high']) & (df['high'].shift(4) <= df['high']) & (df['high'].shift(3) <= df['high']) & (df['high'].shift(2) <= df['high']) & (df['high'].shift(1) <= df['high']) &
                                  (df['high'].shift(-1) <= df['high']) & (df['high'].shift(-2) <= df['high']) & (df['high'].shift(-3) <= df['high']) & (df['high'].shift(-4) <= df['high']) & (df['high'].shift(-5) <= df['high']) & (df['high'].shift(-6) <= df['high'])]
    df['swing_low'] = df['low'][(df['low'].shift(6) >= df['low']) & (df['low'].shift(5) >= df['low']) & (df['low'].shift(4) >= df['low']) & (df['low'].shift(3) >= df['low']) & (df['low'].shift(2) >= df['low']) & (df['low'].shift(1) >= df['low']) &
                                  (df['low'].shift(-1) >= df['low']) & (df['low'].shift(-2) >= df['low']) & (df['low'].shift(-3) >= df['low']) & (df['low'].shift(-4) >= df['low']) & (df['low'].shift(-5) >= df['low']) & (df['low'].shift(-6) >= df['low'])]

    #=======================================This section remove duplicates swings=======================================
    swinghightimframe = (df[['open_time','swing_high']].dropna().index)
    lh1 = [df.index[0]];lh2 = [df.index[-1]];swinghightimframe = [*lh1, *swinghightimframe, *lh2]#adding first and last timetage to the checking list
    swinglowtimframe = (df[['open_time','swing_low']].dropna().index)
    ll1 = [df.index[0]];ll2 = [df.index[-1]];swinglowtimframe = [*ll1, *swinglowtimframe, *ll2]#adding first and last timetage to the checking list

    for i in range(1, len(swinghightimframe)):
        if (df.loc[(df.index >= swinghightimframe[i - 1]) & (df.index <= swinghightimframe[i])])['swing_low'].count() > 1:
            for j in (df.loc[(df.index >= swinghightimframe[i - 1]) & (df.index <= swinghightimframe[i])])['swing_low'].dropna().sort_values(ascending=True)[1:].index:
                df.loc[(df.index == j),['swing_low']] = np.nan
    for i in range(1, len(swinglowtimframe)):
        if (df.loc[(df.index >= swinglowtimframe[i - 1]) & (df.index <= swinglowtimframe[i])])['swing_high'].count() > 1:
            for j in (df.loc[(df.index >= swinglowtimframe[i - 1]) & (df.index <= swinglowtimframe[i])])['swing_high'].dropna().sort_values(ascending=False)[1:].index:
                df.loc[(df.index == j),['swing_high']] = np.nan
    return df
def swing_detector_prepertion_order2(DataFrame, Order):         #this function get some data from DB and find outs its swing high and low
    # Calculating and preparing Swing dataframe swing will detected after 6 * 5 min candles
    DataFrame['swing_high_order2'] = DataFrame['high'][(DataFrame['high'].shift(2) <= DataFrame['high']) & (DataFrame['high'].shift(1) <= DataFrame['high']) & (DataFrame['high'].shift(-1) <= DataFrame['high']) & (DataFrame['high'].shift(-2) <= DataFrame['high'])]
    DataFrame['swing_low_order2'] = DataFrame['low'][(DataFrame['low'].shift(2) >= DataFrame['low']) & (DataFrame['low'].shift(1) >= DataFrame['low']) & (DataFrame['low'].shift(-1) >= DataFrame['low']) & (DataFrame['low'].shift(-2) >= DataFrame['low'])]

    #This section remove duplicates swings
    swinghightimframe = (DataFrame[['open_time','swing_high_order2']].dropna().index)
    lh1 = [DataFrame.index[0]];lh2 = [DataFrame.index[-1]];swinghightimframe = [*lh1, *swinghightimframe, *lh2] #adding first and last timetage to the checking list
    swinglowtimframe = (DataFrame[['open_time','swing_low_order2']].dropna().index)
    ll1 = [DataFrame.index[0]];ll2 = [DataFrame.index[-1]];swinglowtimframe = [*ll1, *swinglowtimframe, *ll2]#adding first and last timetage to the checking list
    for i in range(1, len(swinghightimframe)):
        if (DataFrame.loc[(DataFrame.index >= swinghightimframe[i - 1]) & (DataFrame.index <= swinghightimframe[i])])['swing_low_order2'].count() > 1:
            for j in (DataFrame.loc[(DataFrame.index >= swinghightimframe[i - 1]) & (DataFrame.index <= swinghightimframe[i])])['swing_low_order2'].dropna().sort_values(ascending=True)[1:].index:
                DataFrame.loc[(DataFrame.index == j),['swing_low_order2']] = np.nan
    for i in range(1, len(swinglowtimframe)):
        if (DataFrame.loc[(DataFrame.index >= swinglowtimframe[i - 1]) & (DataFrame.index <= swinglowtimframe[i])])['swing_high_order2'].count() > 1:
            for j in (DataFrame.loc[(DataFrame.index >= swinglowtimframe[i - 1]) & (DataFrame.index <= swinglowtimframe[i])])['swing_high_order2'].dropna().sort_values(ascending=False)[1:].index:
                DataFrame.loc[(DataFrame.index == j),['swing_high_order2']] = np.nan
    return DataFrame

def linear_regression_support_Resistance(DataFrame, SwingCount, SwingThreshold):
    df1 = DataFrame[['open_time','swing_high']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    df2 = DataFrame[['open_time', 'swing_low']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    X1 = df1[['open_time']];Y1 = df1[['swing_high']]
    X2 = df2[['open_time']];Y2 = df2[['swing_low']]
    lm1 = LinearRegression();lm2 = LinearRegression()
    Res = lm1.fit(X1, Y1);Sup = lm2.fit(X2, Y2)

    DataFrame['LinReg_Resistance']=lm1.predict(DataFrame[['open_time']])
    DataFrame['LinReg_Support'] = lm2.predict(DataFrame[['open_time']])

    DataFrame['Res_Coefficient'] = round(Res.coef_[0][0], 6)
    DataFrame['Sup_Coefficient'] = round(Sup.coef_[0][0], 6)

    #Residual error calculation
    DataFrame['LinRegRes_Residual_Error'] = (DataFrame['swing_high'] - DataFrame['LinReg_Resistance']).fillna(0).replace(0,np.nan)
    DataFrame['LinRegSup_Residual_Error'] = (DataFrame['swing_low'] - DataFrame['LinReg_Support']).fillna(0).replace(0, np.nan)

    DataFrame = DataFrame[(DataFrame['open_time'] >= min([df1[['open_time']].iloc[0, 0], df2[['open_time']].iloc[0, 0]]))]  #Just keep the desired swings data and drop the rest

    return DataFrame

def sub_linear_regression_support_Resistance(DataFrame, SwingCount, SwingThreshold):
    df1 = DataFrame[['Open_Time_Sec', 'Swing_High_Order2']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    df2 = DataFrame[['Open_Time_Sec', 'Swing_Low_Order2']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    X1 = df1[['Open_Time_Sec']];Y1 = df1[['Swing_High_Order2']]
    X2 = df2[['Open_Time_Sec']];Y2 = df2[['Swing_Low_Order2']]
    lm1 = LinearRegression();lm2 = LinearRegression()
    Res = lm1.fit(X1, Y1);Sup = lm2.fit(X2, Y2)

    DataFrame.loc[DataFrame['Open_Time_Sec'] >= (df1['Open_Time_Sec'].head(1).values[0]), ['Sub_LinReg_Resistance']] = \
        lm1.predict(((DataFrame[(DataFrame['Open_Time_Sec'] >= (df1['Open_Time_Sec'].head(1).values[0]))])[['Open_Time_Sec']]))
    DataFrame.loc[DataFrame['Open_Time_Sec'] >= (df1['Open_Time_Sec'].head(1).values[0]), ['Sub_LinReg_Support']] = \
        lm2.predict(((DataFrame[(DataFrame['Open_Time_Sec'] >= (df1['Open_Time_Sec'].head(1).values[0]))])[['Open_Time_Sec']]))

    return DataFrame
def polynomial_regression_support_Resistance(DataFrame, SwingCount, SwingThreshold, degree):
    df1 = DataFrame[['Open_Time_Sec', 'Swing_High']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    df2 = DataFrame[['Open_Time_Sec', 'Swing_Low']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    X1=df1["Open_Time_Sec"];Y1=df1["Swing_High"]
    X2=df2["Open_Time_Sec"];Y2=df2["Swing_Low"]
    mymodel1 = np.poly1d(np.polyfit(X1, Y1, degree))
    mymodel2 = np.poly1d(np.polyfit(X2, Y2, degree))
    DataFrame['PolyReg_Resistance'] = mymodel1(DataFrame['Open_Time_Sec'])
    DataFrame['PolyReg_Support'] = mymodel2(DataFrame['Open_Time_Sec'])
    return DataFrame
def sub_polynomial_regression_support_Resistance(DataFrame, SwingCount, SwingThreshold, degree):
    df1 = DataFrame[['Open_Time_Sec', 'Swing_High_Order2']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    df2 = DataFrame[['Open_Time_Sec', 'Swing_Low_Order2']].dropna().tail(SwingCount).head(SwingCount-SwingThreshold)
    X1=df1["Open_Time_Sec"];Y1=df1["Swing_High_Order2"]
    X2=df2["Open_Time_Sec"];Y2=df2["Swing_Low_Order2"]
    mymodel1 = np.poly1d(np.polyfit(X1, Y1, degree))
    mymodel2 = np.poly1d(np.polyfit(X2, Y2, degree))
    #DataFrame['Sub_PolyReg_Resistance'] = mymodel1(DataFrame['Open_Time_Sec'])
    #DataFrame['Sub_PolyReg_Support'] = mymodel2(DataFrame['Open_Time_Sec'])
    DataFrame.loc[DataFrame['Open_Time_Sec'] >= (df1['Open_Time_Sec'].head(1).values[0]), ['Sub_PolyReg_Resistance']] = \
        mymodel1((DataFrame[(DataFrame['Open_Time_Sec']>=(df1['Open_Time_Sec'].head(1).values[0]))])[['Open_Time_Sec']])
    DataFrame.loc[DataFrame['Open_Time_Sec'] >= (df1['Open_Time_Sec'].head(1).values[0]), ['Sub_PolyReg_Support']] = \
        mymodel2((DataFrame[(DataFrame['Open_Time_Sec']>=(df1['Open_Time_Sec'].head(1).values[0]))])[['Open_Time_Sec']])
    return DataFrame
def trend_power_calculation(DataFrame):
    #Close position calculate 3-states of High Close, Mid Close and Low Close and return a number between -1 to +1 (Data Normalization/-1~-0.34 low close/-0.34~+0.34 mid close/+0.34~1 high close)
    DataFrame['Close_Position_Power'] = ((DataFrame['Close'] - (((DataFrame['High']-DataFrame['Low'])/2)+DataFrame['Low'])) * 2)  / (DataFrame['High']-DataFrame['Low'])  # Formula ((C-M)*2)/(H-L) = -1 to +1

    #Close comparison power (if the close is higher than range of previouse candle return 1 if it inside the range retuen 0 and if lower return -1 )
    DataFrame.loc[(DataFrame['Close'] > DataFrame['High'].shift(1)), ['Close_Comparison_Power']] = 1
    DataFrame.loc[(DataFrame['Close'] < DataFrame['Low'].shift(1)), ['Close_Comparison_Power']] = -1
    DataFrame.loc[(DataFrame['Close'] > DataFrame['Low'].shift(1)) & (DataFrame['Close'] < DataFrame['High'].shift(1)), ['Close_Comparison_Power']] = 0

    #Body Power calculation
    DataFrame.loc[(DataFrame['Close'] > DataFrame['Open']), ['Candle_Body_Power']] = (DataFrame['Close'] - DataFrame['Open']) / (DataFrame['Close'] - DataFrame['Open']).max()
    DataFrame.loc[(DataFrame['Close'] < DataFrame['Open']), ['Candle_Body_Power']] = ((DataFrame['Open'] - DataFrame['Close']) / (DataFrame['Open'] - DataFrame['Close']).max()) * (-1)

    #Candles colors
    DataFrame.loc[(DataFrame['Close'] < DataFrame['Open']), ['Candle_Color']] = "red"
    DataFrame.loc[(DataFrame['Close'] > DataFrame['Open']), ['Candle_Color']] = "green"
    DataFrame.loc[(DataFrame['Close'] == DataFrame['Open']), ['Candle_Color']] = "gray"

    return DataFrame
def alarm_orderplace_function():
    for i, Symbol in enumerate(markets):        #getthing live price od markets
        try:
            price = (session.query_kline(symbol=Symbol, interval="5", limit=1, from_time=(int(time.time()) - 300)))['result'][0]['close']
        except:
            break

        con = sqlite3.connect("ST2_DB.db")
        df = pd.read_sql('SELECT * FROM Table_' + Symbol, con)
        con.close()
        index = markets.index(Symbol)

        #Set 1st Conditions
        if (df['LinReg_Resistance'].iloc[-1]+df['LinRegRes_Residual_Error'].max() >= price) and (df['LinReg_Resistance'].iloc[-1] <= price) and (df['Res_Coefficient'].iloc[-1] <= 0) and (FirstShortConditionLatch[index] == False):
            FirstShortConditionLatch[index] = True
            #winsound.Beep(3000, 1000);time.sleep(0.3);winsound.Beep(3500, 1000);winsound.Beep(3000, 1000)
            print(datetime.now(), " ",Symbol, " ", price, " Short 1st condition meet")
            #print("Resistance shadows: ", df['LinReg_Resistance'].iloc[-1], (df['LinReg_Resistance'].iloc[-1] + df['LinRegRes_Residual_Error'].max()), "MAX RE: ", df['LinRegRes_Residual_Error'].max(),
                  #" / ", (df['LinReg_Resistance'].iloc[-1] + df['LinRegRes_Residual_Error'].min()), "MIN RE: ", df['LinRegRes_Residual_Error'].min())
            #print("Max winning percentage: ", round(abs(100-((df['LinReg_Support'].iloc[-1])*100)/(df['LinReg_Resistance'].iloc[-1])), 2), "%")
            #print("Max losing short percentage: ", round(abs(100-(((df['LinReg_Resistance'].iloc[-1] + df['LinRegRes_Residual_Error'].min())*100)/df['LinReg_Resistance'].iloc[-1])), 2), "%")
            #print("=========================")
        elif (df['LinReg_Support'].iloc[-1] >= price) and (df['LinReg_Support'].iloc[-1]+df['LinRegSup_Residual_Error'].min() <= price) and (df['Sup_Coefficient'].iloc[-1] >= 0) and (FirstLongConditionLatch[index] == False):
            FirstLongConditionLatch[index] = True
            #winsound.Beep(3000, 1000);time.sleep(0.3);winsound.Beep(3500, 1000);winsound.Beep(3000, 1000)
            print(datetime.now(), " ",Symbol, " ", price, " Long 1st condition meet")
            #print("Support shadow: ", df['LinReg_Support'].iloc[-1], (df['LinReg_Support'].iloc[-1] + df['LinRegSup_Residual_Error'].max()),"MAX RE: ", df['LinRegSup_Residual_Error'].max(),
                  #" / ", (df['LinReg_Support'].iloc[-1] + df['LinRegSup_Residual_Error'].min()), "MIN RE: ", df['LinRegSup_Residual_Error'].min())
            #print("Max winning percentage: ", round(abs(100-((df['LinReg_Support'].iloc[-1])*100)/(df['LinReg_Resistance'].iloc[-1])), 2), "%")
            #print("Max losing long percentage: ", round(abs(100-(((df['LinReg_Support'].iloc[-1] + df['LinRegSup_Residual_Error'].min())*100)/df['LinReg_Support'].iloc[-1])), 2), "%")
            #print("=========================")

        #Reset 1st Conditions
        if (FirstShortConditionLatch[index] == True) and (df['LinReg_Resistance'].iloc[-1]+df['LinRegRes_Residual_Error'].max()<price):
            FirstShortConditionLatch[index] = False
            print(datetime.now(), " ",Symbol, " ", price, " Short 1st condition reset")
        elif (FirstLongConditionLatch[index] == True) and (df['LinReg_Support'].iloc[-1]+df['LinRegSup_Residual_Error'].min()>price):
            FirstLongConditionLatch[index] = False
            print(datetime.now(), " ",Symbol, " ", price, " Long 1st condition reset")

        #Set 2nd Condition
        if (FirstShortConditionLatch[index] == True) and (df['LinReg_Resistance'].iloc[-1]>price) and (SecondShortConditionLatch[index] == False):
            FirstShortConditionLatch[index] = False
            SecondShortConditionLatch[index] = True
            print(datetime.now(), " ",Symbol, " ", price, " Short 2nd condition meet")
        elif (FirstLongConditionLatch[index] == True) and (df['LinReg_Support'].iloc[-1]<price) and (SecondLongConditionLatch[index] == False):
            FirstLongConditionLatch[index] = False
            SecondLongConditionLatch[index] = True
            print(datetime.now(), " ",Symbol, " ", price, " Long 2nd condition meet")

        #Place order
        #if
        #elif
def swing_detector_prepertion_new(DataFrame, Order):
    # Calculating and preparing Swing dataframe swing
    max_idx = argrelextrema(DataFrame['High'].values, np.greater_equal, order=Order)[0]
    min_idx = argrelextrema(DataFrame['Low'].values, np.less_equal, order=Order)[0]
    DataFrame['Swing_High'] = DataFrame.iloc[max_idx]['High']
    DataFrame['Swing_Low'] = DataFrame.iloc[min_idx]['Low']
    return DataFrame
#==============================================MAIN=====================================================================
url="https://api.bybit.com"
session = usdt_perpetual.HTTP(url)

while FirstTimeRun:
    datetime_object = datetime.now()
    if not datetime_object.minute % 5 and datetime_object.second == 0:
        print('********************Getting Cyclic Data Starts********************')
        schedule.every(5).minutes.at(":00").do(job)  # Task Scheduler
        print("Run time: ", datetime.now())
        job()
        FirstTimeRun = False
        break
    time.sleep(1)
while True:
    #alarm_orderplace_function()
    schedule.run_pending()
    time.sleep(7)

