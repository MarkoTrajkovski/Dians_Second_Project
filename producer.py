import os
import sys
import json
import pandas as pd
from confluent_kafka import Producer

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092'
}

# Create Kafka Producer
def create_producer():
    try:
        producer = Producer(KAFKA_CONFIG)
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        sys.exit(1)

# Define the file path inside WSL
FILE_PATH = os.path.expanduser(r"C:\Users\BUCO\PycharmProjects\JupyterProject\tmx_symbols.csv")

if not os.path.exists(FILE_PATH):
    print(f"Error: The file {FILE_PATH} does not exist!")
    sys.exit(1)

print(f"File {FILE_PATH} found, proceeding...")

# Initialize Kafka Producer
producer = create_producer()

# Read CSV file
def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        return df.to_dict(orient='records')  # Convert to list of dicts
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)

# Send data to Kafka
def send_to_kafka(producer, topic, data):
    try:
        for record in data:
            producer.produce(topic, key=str(record['Symbol']), value=json.dumps(record))
            print(f"Sent: {record}")
        producer.flush()
        print("All messages sent successfully!")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
        sys.exit(1)

# Main execution
if __name__ == "__main__":
    data = read_csv(FILE_PATH)
    send_to_kafka(producer, "tmx_topic", data)
