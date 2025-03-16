import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
import datetime

# ✅ Initialize Dash App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.title = "Live Stock Dashboard"

# ✅ PostgreSQL Database Connection
DB_CONNECTION = "postgresql://postgres:HappyFriday%4021@localhost:5432/stock_data"

# ✅ Function to Fetch Stock Data
def fetch_stock_data(query):
    engine = create_engine(DB_CONNECTION)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Database error: {e}")
        return pd.DataFrame()

# ✅ Fetch Available Stock Symbols
symbol_query = "SELECT DISTINCT yahoo_symbol FROM stock_prices ORDER BY yahoo_symbol;"
stocks_df = fetch_stock_data(symbol_query)
stock_symbols = stocks_df['yahoo_symbol'].tolist()

# ✅ Default Stock Symbols in Case of Database Failure
if not stock_symbols:
    stock_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# ✅ Layout
app.layout = dbc.Container([
    html.H1("📈 Live Stock Price Dashboard", className="text-center mt-4 mb-3"),

    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id='live-stock-selector',
                options=[{'label': symbol, 'value': symbol} for symbol in stock_symbols],
                value=stock_symbols[0] if stock_symbols else None,
                clearable=False,
                searchable=True,
                placeholder="Search for a stock symbol...",
                className="mb-3",
                style={'color': 'black', 'fontSize': '18px', 'width': '100%'}
            ),
            dcc.Graph(id='live-stock-chart', config={'displayModeBar': False}),
        ], width=8),

        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("📊 Stock Information", className="card-title"),
                    html.H2(id="stock-price", className="text-success"),
                    html.H5(id="stock-change", className="text-muted"),
                    html.P("Last updated:", className="mt-2"),
                    html.P(id="last-updated", className="text-info"),
                ])
            ], className="mb-3")
        ], width=4)
    ], className="mb-5"),

    html.H3("📜 Historical Stock Prices (Last 3 Months)", className="text-center mb-3"),
    dcc.Dropdown(
        id='historical-stock-selector',
        options=[{'label': symbol, 'value': symbol} for symbol in stock_symbols],
        value=stock_symbols[0] if stock_symbols else None,
        clearable=False,
        searchable=True,
        placeholder="Search for a stock symbol...",
        className="mb-3",
        style={'color': 'black', 'fontSize': '18px', 'width': '100%'}
    ),
    dcc.Graph(id='historical-stock-chart', config={'displayModeBar': False}),

    dcc.Interval(id='interval-component', interval=10000, n_intervals=0),
], fluid=True)

# ✅ Live Stock Price Callback (Now Includes "Last Price")
@app.callback(
    [Output('live-stock-chart', 'figure'),
     Output('stock-price', 'children'),
     Output('stock-change', 'children'),
     Output('last-updated', 'children')],
    [Input('live-stock-selector', 'value'), Input('interval-component', 'n_intervals')]
)
def update_live_graph(selected_symbol, n_intervals):
    if not selected_symbol:
        return go.Figure(), "N/A", "No Data Available", "N/A"

    # ✅ Fetch "Last Price" (real-time) and "Close Price" (historical)
    query = f"""
        SELECT last_price, close_price, timestamp 
        FROM stock_prices 
        WHERE yahoo_symbol = '{selected_symbol}'
        ORDER BY timestamp DESC
        LIMIT 50
    """
    df = fetch_stock_data(query)

    if df.empty:
        return go.Figure(), "N/A", "No Data Available", "N/A"

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['last_price'] = pd.to_numeric(df['last_price'])
    df['close_price'] = pd.to_numeric(df['close_price'])

    # ✅ Get latest stock price & timestamp
    last_updated = df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S')

    # ✅ Create Live Stock Price Chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['last_price'],  # ✅ Now using "Last Price"
        mode='lines+markers',
        name=selected_symbol,
        line=dict(color='lime', width=2),
        marker=dict(size=6, color="yellow")
    ))

    # ✅ Compute Price Change Percentage
    latest_price = df['last_price'].iloc[-1]
    prev_price = df['last_price'].iloc[-2] if len(df) > 1 else latest_price
    change_pct = ((latest_price - prev_price) / prev_price) * 100 if prev_price else 0

    change_text = f"({change_pct:+.2f}%)"
    price_text = f"${latest_price:.2f}"

    fig.update_layout(
        title=f"📊 {selected_symbol} - Live Price",
        xaxis_title="Time",
        yaxis_title="Stock Price",
        template="plotly_dark"
    )

    return fig, price_text, change_text, last_updated

# ✅ Historical Stock Price Callback
@app.callback(
    Output('historical-stock-chart', 'figure'),
    [Input('historical-stock-selector', 'value')]
)
def update_historical_graph(selected_symbol):
    if not selected_symbol:
        return go.Figure()

    three_months_ago = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    query = f"""
        SELECT close_price, timestamp 
        FROM stock_prices 
        WHERE yahoo_symbol = '{selected_symbol}'
        AND timestamp >= '{three_months_ago}'
        ORDER BY timestamp ASC
    """
    df = fetch_stock_data(query)

    if df.empty:
        return go.Figure()

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['close_price'] = pd.to_numeric(df['close_price'])

    # ✅ Group by day to get daily close prices
    df['date'] = df['timestamp'].dt.date
    daily_df = df.groupby('date').agg({'close_price': 'last'}).reset_index()
    daily_df['date'] = pd.to_datetime(daily_df['date'])

    # ✅ Create Historical Stock Price Chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_df['date'],
        y=daily_df['close_price'],
        mode='lines+markers',
        name=selected_symbol,
        line=dict(color='deepskyblue', width=2),
        marker=dict(size=6, color="yellow")
    ))

    fig.update_layout(
        title=f"📜 {selected_symbol} - Historical Trend (3 Months)",
        xaxis_title="Date",
        yaxis_title="Stock Price",
        template="plotly_dark"
    )

    return fig

# ✅ Run the Dash App
if __name__ == '__main__':
    app.run_server(debug=True)
