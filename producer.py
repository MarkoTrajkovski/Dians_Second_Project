from confluent_kafka import Producer
import pandas as pd
import os
import sys
import json
import time

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092'  # Change if using remote Kafka broker
}

TOPIC = "stock_topic"  # Updated topic name

# Create Kafka producer
def create_producer():
    try:
        producer = Producer(KAFKA_CONFIG)
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        sys.exit(1)

# Define the file path for stock_prices.csv
FILE_PATH = r"C:\Users\BUCO\PycharmProjects\JupyterProject\stock_prices.csv"

if not os.path.exists(FILE_PATH):
    print(f"Error: The file {FILE_PATH} does not exist!")
    sys.exit(1)

print(f"File {FILE_PATH} found, proceeding...")

def send_to_kafka(producer, topic, data):
    try:
        for _, row in data.iterrows():
            # Extract only the necessary columns
            message = {
                "Yahoo Symbol": row["Yahoo Symbol"],
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"]
            }
            producer.produce(topic, key=str(row['Yahoo Symbol']), value=json.dumps(message))
            time.sleep(0.01)  # Prevent overwhelming Kafka
        producer.flush()  # Ensure all messages are delivered
        print("Data successfully sent to Kafka!")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
    finally:
        producer.flush()  # Final flush to ensure all data is sent

if __name__ == "__main__":
    # Initialize Kafka Producer
    producer = create_producer()

    # Read CSV file and keep only required columns
    try:
        data = pd.read_csv(FILE_PATH, usecols=["Yahoo Symbol", "Open", "High", "Low", "Close"])
    except KeyError:
        print("Error: CSV file does not contain the required columns!")
        sys.exit(1)

    # Send data to Kafka
    send_to_kafka(producer, TOPIC, data)

    # Ensure the producer is properly closed
    producer.flush()
