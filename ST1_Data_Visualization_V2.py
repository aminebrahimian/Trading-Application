import pandas as pd
import plotly.express as px  # (version 4.7.0 or higher)
import datetime
import plotly.graph_objects as go
import sqlite3
from dash import Dash, dcc, html, Input, Output  # pip install dash (version 2.0.0 or higher)

app = Dash(__name__)

# -- Import and clean data (importing csv into pandas)
# df = pd.read_csv("intro_bees.csv")
#df = pd.read_csv("https://raw.githubusercontent.com/Coding-with-Adam/Dash-by-Plotly/master/Other/Dash_Introduction/intro_bees.csv")
def data_fetching(exchange, symbol):
    con = sqlite3.connect("Strategy1_DB.db")  # Getting Parameters From DB and turn it to the series
    df1 = pd.read_sql("SELECT * FROM " + exchange + "_" + symbol + "_60", con)
    con.close()
    con = sqlite3.connect("Data_Gathering_DB.db")  # Getting Parameters From DB and turn it to the series
    df2 = pd.read_sql("SELECT * FROM Live_price", con)
    con.close()
    df1['Open_Time'] = pd.to_datetime(df1['open_time'], unit='s');del df1['open_time'];del df1['Time_Tag']
    df1 = df1.set_index('Open_Time')
    live_price = df2[exchange + '_' + symbol].values[0]

    return df1, live_price

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H1("Web Application Dashboards with Dash", style={'text-align': 'center'}),
    dcc.Dropdown(id="slct_exchange",
                 options=[
                     {"label": "BYBIT", "value": "BYBIT"},
                     {"label": "BYBIT", "value": "BYBIT"}],
                 multi=False,
                 value="BYBIT",
                 style={'width': "40%"}
                 ),
    dcc.Dropdown(id="slct_pair",
                 options=[
                     {"label": "BTCUSDT", "value": "BTCUSDT"},
                     {"label": "ETHUSDT", "value": "ETHUSDT"},
                     {"label": "BNBUSDT", "value": "BNBUSDT"},
                     {"label": "LUNA2USDT", "value": "LUNA2USDT"}],
                 multi=False,
                 value="BTCUSDT",
                 style={'width': "40%"}
                 ),

    #html.Br(),  #insert a break for keeping sapace between lines
    html.Div(id='output_container', children=[]),
    #html.Br(),  #insert a break for keeping sapace between lines
    dcc.Graph(id='chart1', figure={}, style={'height': '100vh'}),
    dcc.Interval(id='interval-component', interval=1*5000, n_intervals=0)   # in milliseconds

])


# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
    [Output(component_id='output_container', component_property='children'),
     Output(component_id='chart1', component_property='figure')],
    [Input(component_id='slct_exchange', component_property='value'),
     Input(component_id='slct_pair', component_property='value'),
     Input('interval-component', 'n_intervals')]
)
def update_graph(option_slctd_exchange,option_slctd_pair, interval):
    df,live_price = data_fetching(exchange=option_slctd_exchange, symbol=option_slctd_pair)

    container = option_slctd_exchange + " / " + option_slctd_pair + " / "+str(live_price)

    # Plotly Express
    fig1 = go.Figure()
    fig1.add_trace(go.Candlestick(x=df.index, open=df['open'], high=df['high'], low=df['low'], close=df['close'],name='market data'))
    fig1.update_layout(title=option_slctd_pair, yaxis_title='USDT', showlegend=False)
    fig1.add_trace(go.Scatter(x=df.index, y=df['LinReg_Resistance'], opacity=0.7, line=dict(color='green', width=1),name='LinReg_Resistance'))
    fig1.add_trace(go.Scatter(x=df.index, y=df['LinReg_Support'], opacity=0.7, line=dict(color='green', width=1),name='LinReg_Support'))

    fig1.add_trace(go.Scatter(x=df.index, y=df['swing_high'], mode="markers", marker=dict(size=10, color="MediumPurple", symbol="triangle-up"), name="SH-Order6"))
    fig1.add_trace(go.Scatter(x=df.index, y=df['swing_low'], mode="markers", marker=dict(size=10, color="MediumPurple", symbol="triangle-down"), name="SL-Order6"))

    fig1.add_trace(go.Scatter(x=df.index, y=df['Sub_LinReg_Resistance'], opacity=0.5, line=dict(color='orange', width=1), name='Sub_LinReg_Resistance'))
    fig1.add_trace(go.Scatter(x=df.index, y=df['Sub_LinReg_Support'], opacity=0.5, line=dict(color='orange', width=1), name='Sub_LinReg_Support'))

    fig1.add_trace(go.Scatter(x=df.index, y=df['swing_high_order2'], mode="markers", marker=dict(size=5, color="green"), name="SH-Order2"))
    fig1.add_trace(go.Scatter(x=df.index, y=df['swing_low_order2'], mode="markers", marker=dict(size=5, color="green"), name="SL-Order2"))
    fig1.update_yaxes(automargin=True)

    return container, fig1


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(host= '0.0.0.0', debug=True, port=7777)  #host= '0.0.0.0', for using in local network