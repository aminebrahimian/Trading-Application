from pybit import usdt_perpetual
import sqlite3
import pandas as pd

def Place_Order(Symbol, Position, Leverage, Price, Quantity_Percentage, TP, SL):             #Exchange likes Bybit, Symbol like BTCUSDT,

    con = sqlite3.connect("Users_Parameters.db")
    df = pd.read_sql("SELECT * FROM Exchanges WHERE status == \"enable\"", con) #just get a enable exchanges accounts
    con.close()

    for i, row in df.iterrows(): #gatting exchanges info one by one
        Exchange = row['exchange']; Endpoint = row['url']; Api_key = row['api_key']; Api_secret = row['api_secret']; Status = row['status']


        #============================================Bybit Exchange=====================================================
        if Exchange == "Bybit":                     #this section Belong to Bybit


            #================================Leverage Adjustment section================================================
            try:
                session_auth = usdt_perpetual.HTTP(endpoint=Endpoint, api_key=Api_key, api_secret=Api_secret)
                session_auth.cross_isolated_margin_switch(symbol=Symbol, is_isolated=False, buy_leverage=Leverage, sell_leverage=Leverage)
                message0 = session_auth.cross_isolated_margin_switch(symbol=Symbol, is_isolated=True, buy_leverage=Leverage, sell_leverage=Leverage)
                #session_auth.set_leverage(symbol=Symbol,buy_leverage=Leverage,sell_leverage=Leverage)
                #print("Buy leverage: ",Leverage,"   Sell leverage: ",Leverage)

            except:
                print("Exchange API Error: ", message0)

            #============================================Order Place====================================================
            if Position == "Long":Side = "Buy"
            elif Position == "Short":Side = "Sell"

            USDTBalance = (float(session_auth.get_wallet_balance(coin="USDT")["result"]["USDT"]["available_balance"]))  #Position Size Calculation
            USDTBalance = USDTBalance - (USDTBalance * 0.05) # deduct 5 percent of total balance
            QuantityUSDT = (Quantity_Percentage * USDTBalance)/100
            Quantity = round(((QuantityUSDT / Price)*Leverage),3)
            print("Order USDT size: ", QuantityUSDT, "Order ", Symbol, " size: ",Quantity)
            try:
                if Quantity > 0:
                    message1 = session_auth.place_active_order(symbol=Symbol, side=Side, order_type="Limit", qty=Quantity, price=Price, time_in_force="GoodTillCancel", reduce_only=False, take_profit=TP, stop_loss=SL, close_on_trigger=False)
                else:
                    print("The token quantity is not correct ", Quantity, "account info ", i, " ",row)
            except:
                print("Exchange API Error: ", message1)


        #============================================Bybit Exchange=====================================================
        if Exchange == "Binance":  # This section Belong to Binance
            print("Sorry, it is not implemented yet")