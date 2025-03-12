from confluent_kafka import Consumer, KafkaException
from pyspark.sql import SparkSession
import json

# Kafka Consumer Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Change if using remote Kafka broker
    'group.id': 'spark-group',
    'auto.offset.reset': 'earliest',
}

TOPIC = "tmx_topic"

def create_consumer():
    """Creates a Kafka consumer using Confluent Kafka."""
    try:
        consumer = Consumer(KAFKA_CONFIG)
        consumer.subscribe([TOPIC])
        print(f"Connected to Kafka topic: {TOPIC}")
        return consumer
    except KafkaException as e:
        print(f"Error creating Kafka consumer: {e}")
        return None

def process_messages(consumer, spark):
    """Reads messages from Kafka and processes them using Spark."""
    try:
        while True:
            msg = consumer.poll(1.0)  # Polls for messages every second
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            # Deserialize message (assuming JSON format)
            record = json.loads(msg.value().decode("utf-8"))
            print(f"Received message: {record}")

            # Convert record into a Spark DataFrame
            df = spark.createDataFrame([record])

            # Perform any required transformation or action
            df.show()

    except KeyboardInterrupt:
        print("Consumer shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .getOrCreate()

    # Create Kafka Consumer
    consumer = create_consumer()
    if consumer:
        process_messages(consumer, spark)
