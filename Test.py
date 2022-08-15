import pandas as pd
import sqlite3
import Test1
MaxProcessDuration= 1.534534
#a = round(1.534534, 2)


df_parameters = pd.DataFrame.from_dict({"max_process_duration": [round(MaxProcessDuration, 2)+1], "last_process_duration": [LastProcessDuration]})
print(df_parameters)


