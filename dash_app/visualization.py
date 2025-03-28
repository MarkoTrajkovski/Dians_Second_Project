import dash
import pytz
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import yfinance as yf  # ✅ NEW: Fetch Live Stock Prices
from sqlalchemy import create_engine
import datetime

# ✅ Initialize Dash App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.title = "Live Stock Dashboard"

# ✅ PostgreSQL Database Connection
DB_CONNECTION = "postgresql://BUCO:Markomarko123@azure22.postgres.database.azure.com:5432/stock_data"


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

# Dynamically pull from database
if 'yahoo_symbol' in stocks_df.columns and not stocks_df.empty:
    stock_symbols = stocks_df['yahoo_symbol'].unique().tolist()
else:
    print("⚠️ Could not fetch symbols — defaulting to fallback list")
    stock_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# ✅ Store Live Data Over Time (For Chart)
def is_market_open():
    eastern = pytz.timezone('US/Eastern')
    now = datetime.datetime.now(eastern)
    today = now.strftime('%Y-%m-%d')

    # Market hours: 9:30 AM - 4:00 PM Eastern
    market_open = eastern.localize(datetime.datetime.strptime(f"{today} 09:30:00", '%Y-%m-%d %H:%M:%S'))
    market_close = eastern.localize(datetime.datetime.strptime(f"{today} 16:00:00", '%Y-%m-%d %H:%M:%S'))

    # Check if it's a weekday (0-4 are Monday to Friday)
    is_weekday = now.weekday() < 5

    # Check if current time is within market hours
    is_market_time = market_open <= now <= market_close

    return is_weekday and is_market_time


def fetch_intraday_data(symbol):
    try:
        query = f"""
            SELECT * FROM intraday_prices 
            WHERE yahoo_symbol = '{symbol}'
            AND timestamp::date = CURRENT_DATE
            ORDER BY timestamp ASC
        """
        df = fetch_stock_data(query)

        if df.empty:
            print(f"No intraday data found for {symbol}")
            return None

        return df
    except Exception as e:
        print(f"Error fetching intraday data: {e}")
        return None

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

    # ✅ Add ML Predictions Graph at the Bottom
    html.H3("🔮 Machine Learning Predictions", className="text-center mb-3"),
    dcc.Graph(id='ml-predictions-chart', config={'displayModeBar': False}),

    dcc.Interval(id='interval-component', interval=15 * 1000, n_intervals=0),
], fluid=True)

# ✅ Live Stock Price Callback (Fixed)
# ✅ Live Stock Price Callback (Restoring Functionality)
@app.callback(
    [Output('live-stock-chart', 'figure'),
     Output('stock-price', 'children'),
     Output('stock-change', 'children'),
     Output('last-updated', 'children')],
    [Input('live-stock-selector', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_live_graph(selected_symbol, n_intervals):
    if not selected_symbol:
        return go.Figure(), "N/A", "No Data Available", "N/A"

    market_status = "OPEN" if is_market_open() else "CLOSED"

    try:
        eastern = pytz.timezone('US/Eastern')
        now = datetime.datetime.now(eastern)

        df = fetch_intraday_data(selected_symbol)
        if df is None or df.empty:
            fig = go.Figure()
            fig.update_layout(
                title=f"No intraday data available for {selected_symbol}",
                xaxis_title="Time",
                yaxis_title="Price",
                template="plotly_dark"
            )
            fig.add_annotation(
                text=f"Market Status: {market_status}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20, color="white")
            )
            return fig, "N/A", "No Data Available", f"Market Status: {market_status} - {now.strftime('%H:%M:%S')}"

        times = df['time'].tolist()
        prices = df['close_price'].tolist()
        latest_price = df['close_price'].iloc[-1]
        open_price = df['open_price'].iloc[0]
        change = latest_price - open_price
        change_pct = (change / open_price) * 100 if open_price > 0 else 0

        price_text = f"${latest_price:.6f}"
        change_text = f"({change_pct:+.6f}%)"
        last_updated = now.strftime('%Y-%m-%d %H:%M:%S') + f" - Market: {market_status} (Refresh {n_intervals})"

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=times,
            y=prices,
            mode='lines+markers',
            line=dict(color='lime', width=2),
            marker=dict(size=6, color="yellow")
        ))

        fig.add_shape(
            type="line",
            x0=times[0],
            y0=open_price,
            x1=times[-1],
            y1=open_price,
            line=dict(color="gray", width=1, dash="dash"),
        )

        fig.add_annotation(
            x=times[0],
            y=open_price,
            text=f"Open: ${open_price:.2f}",
            showarrow=True,
            arrowhead=1
        )

        fig.update_layout(
            title=f"📊 {selected_symbol} - Live Price (Today's Session)",
            xaxis_title="Time (HH:MM:SS)",
            yaxis_title="Stock Price",
            template="plotly_dark",
            hovermode="x unified"
        )

        change_color = "green" if change >= 0 else "red"
        fig.add_annotation(
            x=times[-1],
            y=latest_price,
            text=f"${latest_price:.2f} ({change_pct:+.2f}%)",
            showarrow=True,
            arrowhead=1,
            bgcolor=change_color,
            font=dict(color="white")
        )

        return fig, price_text, change_text, last_updated

    except Exception as e:
        print(f"❌ Live price fetch error: {e}")
        fig = go.Figure()
        fig.update_layout(
            title=f"Error fetching live data for {selected_symbol}",
            template="plotly_dark"
        )
        return fig, "N/A", "Error Fetching Data", f"Error: {str(e)} - Tick {n_intervals}"


# ✅ Historical Stock Price Callback
@app.callback(
    Output('historical-stock-chart', 'figure'),
    [Input('historical-stock-selector', 'value')]
)
def update_historical_graph(selected_symbol):
    if not selected_symbol:
        return go.Figure()

    # Remove time-based filtering that might limit data
    start_date = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')

    query = f"""
        SELECT close_price, timestamp 
        FROM stock_prices 
        WHERE yahoo_symbol = '{selected_symbol}'
        AND timestamp >= '{start_date}'
        ORDER BY timestamp ASC
    """
    df = fetch_stock_data(query)

    if df.empty:
        return go.Figure()

    # Ensure timestamp is converted correctly
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['close_price'] = pd.to_numeric(df['close_price'])
    df['date'] = df['timestamp'].dt.date

    # Group by date, get last close price per day
    daily_df = df.groupby('date').agg({'close_price': 'last'}).reset_index()
    daily_df['date'] = pd.to_datetime(daily_df['date'])

    # Remove zero or null values
    daily_df = daily_df[daily_df['close_price'] > 0]

    # Sort by date to ensure correct chronological order
    daily_df = daily_df.sort_values("date")

    # Plotting code remains the same
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_df['date'],
        y=daily_df['close_price'],
        mode='lines+markers',
        name=selected_symbol,
        line=dict(color='deepskyblue', width=2),
        marker=dict(size=6, color="yellow")
    ))

    # Add most recent date annotation
    if not daily_df.empty:
        last_point = daily_df.iloc[-1]
        fig.add_annotation(
            x=last_point['date'],
            y=last_point['close_price'],
            text=f"Last: {last_point['date'].strftime('%b %d, %Y')} (${last_point['close_price']:.2f})",
            showarrow=True,
            arrowhead=1,
            bgcolor="black",
            font=dict(color="white")
        )

    fig.update_layout(
        title=f"📜 {selected_symbol} - Historical Trend (3 Months)",
        xaxis_title="Date",
        yaxis_title="Stock Price",
        template="plotly_dark"
    )

    return fig


# ✅ ML Predictions Callback (Fixed Dark Theme)
@app.callback(
    Output('ml-predictions-chart', 'figure'),
    [Input('historical-stock-selector', 'value')]
)
def update_ml_graph(selected_symbol):
    if not selected_symbol:
        return go.Figure()

    query = f"""
        SELECT predicted_date, predicted_price 
        FROM stock_predictions 
        WHERE yahoo_symbol = '{selected_symbol}'
        AND predicted_date >= CURRENT_DATE
        ORDER BY predicted_date ASC
    """
    df = fetch_stock_data(query)

    if df.empty:
        return go.Figure()

    df['predicted_date'] = pd.to_datetime(df['predicted_date'])
    df['predicted_price'] = pd.to_numeric(df['predicted_price'])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['predicted_date'],
        y=df['predicted_price'],
        mode='lines+markers',
        name=f"Predicted Price ({selected_symbol})",
        line=dict(color='orange', width=2),
        marker=dict(size=6, color="yellow")
    ))

    fig.update_layout(
        title=f"🔮 Predicted Prices for {selected_symbol} (Next 7 Days)",
        xaxis_title="Date",
        yaxis_title="Predicted Stock Price",
        template="plotly_dark"
    )

    return fig
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)


