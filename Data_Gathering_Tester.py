import sqlite3
import time
import pandas as pd

DB_Name = 'Data_Gathering_DB.db'
con = sqlite3.connect(DB_Name)
Existing_Tables_List = pd.read_sql('SELECT name from sqlite_master where type= \"table\"', con)

for i in Existing_Tables_List['name'].values.tolist():
    if i == 'Parameters': break
    fail_status = False
    df = pd.read_sql('SELECT open_time from ' + i, con)
    tf = int(i.split('_')[-1].replace('m', ''))
    for j in range (1, len(df)):
        diff = (df['open_time'].iloc[j] - df['open_time'].iloc[j-1])
        if (df['open_time'].iloc[j] - df['open_time'].iloc[j-1]) != (tf*60):
            print(i)
            print(df['open_time'].iloc[j] - df['open_time'].iloc[j-1])
            print("Error in row: ", j, df['open_time'].iloc[j])
            fail_status = True
    try:
        if (int(time.time()) - (df['open_time'].iloc[-1]+(tf*60))) > (tf*60):
            print(i)
            print("Error, last row is missed in table ", i)
            fail_status = True
    except:
        pass
    print(fail_status, "=============================================")
con.close()