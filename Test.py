import pandas as pd
import sqlite3

con = sqlite3.connect('DB_Name')
df1 = pd.read_sql('SELECT * FROM BINANCE_BNBUSDT_5', con)