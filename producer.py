from confluent_kafka import Producer
import pandas as pd
import json
import time

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 500,
    'batch.num.messages': 1000
}

TOPIC = "stock_topic"

def create_producer():
    return Producer(KAFKA_CONFIG)

def read_csv():
    try:
        data = pd.read_csv("stock_prices.csv", usecols=["Yahoo Symbol", "Date", "Open", "High", "Low", "Close"])
        data.rename(columns={"Yahoo Symbol": "yahoo_symbol", "Date": "timestamp",
                             "Open": "open_price", "High": "high_price",
                             "Low": "low_price", "Close": "close_price"}, inplace=True)
        return data
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return pd.DataFrame()

def send_to_kafka(producer, topic, data):
    for _, row in data.iterrows():
        message = {
            "yahoo_symbol": row["yahoo_symbol"],
            "open_price": row["open_price"],
            "high_price": row["high_price"],
            "low_price": row["low_price"],
            "close_price": row["close_price"],
            "timestamp": row["timestamp"]
        }
        producer.produce(topic, json.dumps(message).encode("utf-8"))
        print(f"Sent: {message}")
        time.sleep(0.05)

    producer.flush()
    print("All messages sent successfully!")


producer = create_producer()
while True:
    print("\nChecking for new stock data...")
    stock_data = read_csv()
    if not stock_data.empty:
        send_to_kafka(producer, TOPIC, stock_data)
    time.sleep(300)
