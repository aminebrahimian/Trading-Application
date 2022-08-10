import sqlite3
import pandas as pd

DB_Name = 'Data_Gathering_DB.db'

con = sqlite3.connect(DB_Name)
Existing_Tables_List = pd.read_sql('SELECT name from sqlite_master where type= \"table\"', con)

for i in Existing_Tables_List['name'].values.tolist():
    print(i)
    df1 = pd.read_sql('SELECT open_time from ' + i, con)
    df2 = df1.shift(-1)
    df1.drop(df1.tail(1).index, inplace=True)  # drop last n rows
    df2.drop(df2.tail(1).index, inplace=True)  # drop last n rows
    for j in range (0, len(df1)):
        if df2['open_time'].iloc[j] != df1['open_time'].iloc[j]:
            print("Error in row: ", j)




    print("=============================================")

con.close()