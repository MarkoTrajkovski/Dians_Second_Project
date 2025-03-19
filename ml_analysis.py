import psycopg2
import pandas as pd
import numpy as np
import schedule
import time
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine, text


DB_URL = "postgresql://postgres:HappyFriday%4021@localhost:5432/stock_data"
engine = create_engine(DB_URL)



def run_ml_predictions():
    print("\nRunning ML Predictions...")

    query = """
    SELECT yahoo_symbol, timestamp, close_price
    FROM stock_prices
    ORDER BY yahoo_symbol, timestamp ASC;
    """

    df = pd.read_sql(query, engine)
    if df.empty:
        print("No stock data found! Skipping predictions.")
        return

    predictions = []
    grouped = df.groupby("yahoo_symbol")

    for symbol, data in grouped:
        data = data.dropna().sort_values('timestamp').drop_duplicates(subset=['timestamp'])
        if len(data) < 10:
            print(f"Skipping {symbol}: Not enough historical data.")
            continue

        data['days_since_start'] = (data['timestamp'] - data['timestamp'].min()).dt.days
        X = data[['days_since_start']].values
        y = data['close_price'].values

        model = LinearRegression()
        model.fit(X, y)

        for i in range(1, 8):
            future_day = np.array([[data['days_since_start'].max() + i]])
            predicted_price = model.predict(future_day)[0]

            predictions.append({
                "yahoo_symbol": symbol,
                "predicted_date": (datetime.now().date() + timedelta(days=i)).strftime('%Y-%m-%d'),

                "predicted_price": float(predicted_price)
            })

    if predictions:
        with engine.begin() as connection:
            upsert_query = text("""
                INSERT INTO stock_predictions (yahoo_symbol, predicted_date, predicted_price)
                VALUES (:yahoo_symbol, :predicted_date, :predicted_price)
                ON CONFLICT (yahoo_symbol, predicted_date) 
                DO UPDATE SET predicted_price = EXCLUDED.predicted_price;
            """)
            connection.execute(upsert_query, predictions)

        print(f"{len(predictions)} predictions stored/updated in database.")



schedule.every(5).minutes.do(run_ml_predictions)
run_ml_predictions()
while True:
    schedule.run_pending()
    time.sleep(60)
