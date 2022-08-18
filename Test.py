import sqlite3
import pandas as pd

def data_fetching1():
    con = sqlite3.connect("ST2_DB.db")  # Getting Parameters From DB and turn it to the series
    Data1 = pd.read_sql("SELECT * FROM Table_BTCUSDT", con)
    con.close()
    del Data1['Open_Time']
    Data1['Open_Time'] = pd.to_datetime(Data1['Open_Time_Sec'], unit='s')  # changing time format from seconds to time and date
    Data1 = Data1.set_index('Open_Time')

    return Data1
def data_fetching2():
    con = sqlite3.connect("Strategy1_DB.db")  # Getting Parameters From DB and turn it to the series
    Data1 = pd.read_sql("SELECT * FROM BYBIT_BTCUSDT_5", con)
    con.close()
    Data1.rename(columns={'open_time':'Open_Time_Sec', 'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume', 'swing_high':'Swing_High',
       'swing_low':'Swing_Low', 'swing_high_order2':'Swing_High_Order2', 'swing_low_order2':'Swing_Low_Order2','LinReg_Resistance':'LinReg_Resistance', 'LinReg_Support':'LinReg_Support', 'Res_Coefficient':'Res_Coefficient',
       'Sup_Coefficient':'Sup_Coefficient', 'LinRegRes_Residual_Error':'LinRegRes_Residual_Error','LinRegSup_Residual_Error':'LinRegSup_Residual_Error', 'Sub_LinReg_Resistance':'Sub_LinReg_Resistance',
       'Sub_LinReg_Support':'Sub_LinReg_Support', 'PolyReg_Resistance':'PolyReg_Resistance', 'PolyReg_Support':'PolyReg_Support','Sub_PolyReg_Resistance':'Sub_PolyReg_Resistance', 'Sub_PolyReg_Support':'Sub_PolyReg_Support', 'close_position_power':'Close_Position_Power',
       'close_comparison_power':'Close_Comparison_Power', 'candle_body_power':'Candle_Body_Power', 'candle_color':'Candle_Color'}, inplace=True)
    del Data1['Time_Tag']
    Data1['Open_Time'] = pd.to_datetime(Data1['Open_Time_Sec'], unit='s')  # changing time format from seconds to time and date
    Data1 = Data1.set_index('Open_Time')
    Data1["Open_Time_Sec"] = Data1["Open_Time_Sec"].astype(int)
    return Data1
print(data_fetching1().dtypes)
print(data_fetching2().dtypes)