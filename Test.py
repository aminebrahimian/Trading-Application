import pandas as pd
import sqlite3

con = sqlite3.connect('Data_Gathering_DB.db')
df1 = pd.read_sql('SELECT * FROM BINANCE_BNBUSDT_5m', con)
df2 = pd.read_sql('SELECT * FROM BYBIT_BNBUSDT_5m', con)
print(df1.dtypes)
print(df2.dtypes)