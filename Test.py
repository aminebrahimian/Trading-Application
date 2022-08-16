from pybit import usdt_perpetual
import time
import requests

df_prices = pd.DataFrame.from_dict()
con = sqlite3.connect(DB_Name)
df_live_price.to_sql('Live_price', con, if_exists='replace', index=False)
con.close()
# print(exc, " ", symbol, " price: ", price)
print(prices)