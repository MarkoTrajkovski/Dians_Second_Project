from confluent_kafka import Producer
import pandas as pd
import json
import time


KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 200000,
    'queue.buffering.max.kbytes': 1048576,
    'batch.num.messages': 5000,
    'linger.ms': 100,
    'compression.type': 'snappy'
}

TOPIC = "stock_topic"

def create_producer():
    return Producer(KAFKA_CONFIG)

def read_csv():
    try:
        data = pd.read_csv("stock_prices.csv",
                           usecols=["Yahoo Symbol", "Date", "Open", "High", "Low", "Close", "Last Price"])
        data.rename(columns={
            "Yahoo Symbol": "yahoo_symbol",
            "Date": "timestamp",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Last Price": "last_price"
        }, inplace=True)
        return data
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return pd.DataFrame()

def on_delivery(err, msg):
    """ Callback for delivery confirmation """
    if err:
        print(f"Message failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_to_kafka(producer, topic, data):
    """ Sends stock data to Kafka in a batch """
    message_count = 0

    for _, row in data.iterrows():
        message = {
            "yahoo_symbol": row["yahoo_symbol"],
            "open_price": row["open_price"],
            "high_price": row["high_price"],
            "low_price": row["low_price"],
            "close_price": row["close_price"],
            "last_price": row["last_price"],
            "timestamp": row["timestamp"]
        }

        producer.produce(topic, json.dumps(message).encode("utf-8"), callback=on_delivery)
        producer.poll(0)

        message_count += 1
        if message_count % 100 == 0:
            print(f"{message_count} messages sent...")

    producer.flush()
    print(f"Sent {message_count} messages successfully!")


producer = create_producer()

while True:
    print("\nChecking for new stock data...")
    stock_data = read_csv()

    if not stock_data.empty:
        send_to_kafka(producer, TOPIC, stock_data)
    else:
        print("No new stock data found.")

    print("Waiting 5 minutes before next check...\n")
    time.sleep(300)
