import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine

# Create SQLAlchemy engine
DATABASE_URL = "postgresql://postgres:HappyFriday%4021@localhost:5432/stock_data"
engine = create_engine(DATABASE_URL)


# Fetch stock data from PostgreSQL
def fetch_data():
    query = """
    SELECT yahoo_symbol, close_price, timestamp 
    FROM stock_prices 
    WHERE timestamp >= NOW() - INTERVAL '1 hour'  -- Fetch last 1 hour
    ORDER BY timestamp DESC
    """

    df = pd.read_sql(query, con=engine)

    # Convert timestamp column to proper datetime format
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 🔹 Sort data to ensure correct plotting
    df = df.sort_values(by="timestamp")

    print(df.tail(10))  # ✅ Debugging: Ensure the latest timestamps exist

    return df


# Initialize Dash app
app = dash.Dash(__name__)

app.layout = html.Div(children=[
    html.H1("📊 Live Stock Price Dashboard"),

    dcc.Interval(
        id='interval-component',
        interval=5000,  # Refresh every 5 seconds
        n_intervals=0
    ),

    dcc.Graph(id='stock-price-chart')
])


# Callback to update the chart in real time
@app.callback(
    dash.Output('stock-price-chart', 'figure'),
    dash.Input('interval-component', 'n_intervals')
)
def update_graph(n):
    df = fetch_data()

    if df.empty:
        return px.line(title="Waiting for stock data...")

    fig = px.line(df, x="timestamp", y="close_price", color="yahoo_symbol",
                  title="Stock Price Trends (Live)")

    # 🔹 Ensure the graph does not appear blank at start
    fig.update_layout(
        yaxis=dict(range=[df["close_price"].min() * 0.95, df["close_price"].max() * 1.05]),
        xaxis_title="Timestamp",
        yaxis_title="Stock Price",
    )

    return fig



# Run the Dash app
if __name__ == "__main__":
    app.run_server(debug=True)