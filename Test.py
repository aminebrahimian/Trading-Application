import pandas as pd
import sqlite3

con = sqlite3.connect('C:/Users/Amin Laptop/Documents/GitHub/TradingProgram/Bybit/Users_Parameters.db')
df1 = pd.read_sql('SELECT * FROM Exchanges', con)

print(df1)
#C:\Users\Amin Laptop\AppData\Local\Programs\Python\Python310\python.exe
#C:\Users\Amin Laptop\AppData\Local\Microsoft\WindowsApps\python.exe
#C:\Users\Amin Laptop\Documents\GitHub\Trading-Application