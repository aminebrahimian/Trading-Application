import sqlite3
import pandas as pd

DB_Name = 'Data_Gathering_DB.db'

con = sqlite3.connect(DB_Name)
Existing_Tables_List = pd.read_sql('SELECT name from sqlite_master where type= \"table\"', con)

for i in Existing_Tables_List['name'].values.tolist():
    df = pd.read_sql('SELECT open_time from ' + i, con)
    print(df)
    print(df.shift(-1))


con.close()