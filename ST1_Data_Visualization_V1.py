import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import sqlite3
import dash_auth
import datetime
from plotly.subplots import make_subplots
from pybit import usdt_perpetual
import time

global markets
markets = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
url="https://api.bybit.com"
session = usdt_perpetual.HTTP(url)

VALID_USERNAME_PASSWORD_PAIRS = {'admin': 'admin'}

app = Dash(__name__)
app.title = 'HammingCo.'

auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)

# Create figure with secondary y-axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

#Import and clean data (importing db into pandas)
def data_fetching(Symbol):
    con = sqlite3.connect("ST2_DB.db")  # Getting Parameters From DB and turn it to the series
    Data1 = pd.read_sql("SELECT * FROM Table_"+Symbol, con)
    con.close()
    del Data1['Open_Time']
    Data1['Open_Time'] = pd.to_datetime(Data1['Open_Time_Sec'], unit='s')  # changing time format from seconds to time and date
    Data1 = Data1.set_index('Open_Time')

    return Data1

def live_price():
    results = list(range(len(markets)))
    for i, symbol in enumerate(markets):
        try:
            results[i] = (session.query_kline(symbol=symbol, interval="5", limit=1, from_time=(int(time.time()) - 300)))['result'][0]['close']
        except:
            pass
    return [markets, results]

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([
    html.H1("Hamming Co. Trading Web App", style={'text-align': 'center','height':'5px','font-size':20, 'color':'#379ED2'}),
    html.H2(id="date_time", children=[], style={'font-size':12, 'color':'#AEAEBC'}),
    dcc.Dropdown(id="slct_pair", options=[{"label": "BTCUSDT", "value": "BTCUSDT"}, {"label": "ETHUSDT", "value": "ETHUSDT"},
                {"label": "BNBUSDT", "value": "BNBUSDT"}], multi=False, value="BTCUSDT", style={'width': "40%", 'color':'#AEAEBC'}),
    html.Div(id="output_price", children=[]),
    dcc.Checklist(id="check_list", options=[
        {'label': 'Polynomial trend line   ', 'value': 'Polynomial'}, {'label': 'Linear trend line    ', 'value': 'Linear'},
        {'label': 'Polynomial sub trend line    ', 'value': 'PolSubTrend'}, {'label': 'Linear sub trend line    ', 'value': 'LinSubTrend'},
        {'label': 'Main swings    ', 'value': 'MainSwings'}, {'label': 'Sub swings    ', 'value': 'SubSwings'}],
        value=['Linear', 'LinSubTrend', 'MainSwings', 'SubSwings']),

    dcc.Input(id="Entry_Price", type="number", placeholder="Entry price"),
    dcc.Input(id="SL_Price", type="number", placeholder="SL price"),
    dcc.Input(id="TP_Price", type="number", placeholder="TP price"),
    #dcc.Input(id="Quantity", type="number", placeholder="Quantity"),

    html.Br(),

    dcc.Graph(id='Candlestick_Chart1', figure={}, style={'height': '100vh'}),    #, style={'width': '120vh', 'height': '90vh'}

    dcc.Interval(id='interval-component', interval=1*5000, n_intervals=0)   # in milliseconds
])


# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
    [Output(component_id='output_price', component_property='children'),
     Output(component_id='Candlestick_Chart1', component_property='figure'),
     Output(component_id='date_time', component_property='children')],
    [Input(component_id='slct_pair', component_property='value'),
     Input('interval-component', 'n_intervals'),
     Input(component_id='check_list', component_property='value'),])
def update_graph(option_slctd, interval, CheckList):
    df= data_fetching(Symbol=option_slctd)
    symbol, price = live_price()    #getting Live Price
    container = "Live price: {}".format(price[symbol.index(option_slctd)])
    date_time = "Live time: "+ str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Plotly Express
    fig1 = go.Figure()
    fig1.add_trace(go.Candlestick(x=df.index,open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], name='market data'))
    fig1.update_layout(title=option_slctd+' Live Price:', yaxis_title='USDT', showlegend=False)
    fig1.update_yaxes(automargin=True)

    return container, fig1, date_time


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(host= '0.0.0.0', debug=True, port=7777)  #host= '0.0.0.0', for using in same network