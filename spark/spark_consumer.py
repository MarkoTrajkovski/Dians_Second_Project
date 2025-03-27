from confluent_kafka import Consumer, KafkaException
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import psycopg2
import json
import socket
from datetime import datetime, time as dt_time
import time
import pytz
import sys

# PostgreSQL JDBC Configuration
POSTGRES_URL = "jdbc:postgresql://postgres:5432/stock_data"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "HappyFriday@21",
    "driver": "org.postgresql.Driver"
}
JDBC_DRIVER_PATH = "/drivers/postgresql-42.7.2.jar"

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'stock-group',
    'auto.offset.reset': 'earliest'
}
TOPIC = "stock_topic"

# Define schema for the messages
schema = StructType([
    StructField("yahoo_symbol", StringType(), True),
    StructField("open_price", FloatType(), True),
    StructField("high_price", FloatType(), True),
    StructField("low_price", FloatType(), True),
    StructField("close_price", FloatType(), True),
    StructField("last_price", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Ensure target table exists

def ensure_table_exists():
    conn = psycopg2.connect(
        dbname="stock_data",
        user="postgres",
        password="HappyFriday@21",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            yahoo_symbol VARCHAR(20),
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            last_price FLOAT,
            timestamp TIMESTAMP
        );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

# Initialize Spark
spark = SparkSession.builder \
    .appName("KafkaStockConsumer") \
    .config("spark.jars", JDBC_DRIVER_PATH) \
    .getOrCreate()

def wait_for_kafka(host, port, timeout=60):
    print(f"⏳ Waiting for Kafka at {host}:{port}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("✅ Kafka is available!")
                return
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"❌ Timeout waiting for Kafka at {host}:{port}")

wait_for_kafka("kafka", 9092)

# Create Kafka consumer
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([TOPIC])

# Create table if needed
ensure_table_exists()

BATCH_SIZE = 300
batch_data = []
last_flush = time.time()

# Save Spark DataFrame to PostgreSQL

def save_to_postgres(df: DataFrame):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "stock_prices") \
        .options(**POSTGRES_PROPERTIES) \
        .mode("append") \
        .save()
    print("✅ Data successfully saved to PostgreSQL!")

# Process and store a batch of messages

def process_batch():
    global batch_data
    if not batch_data:
        return

    try:
        df = spark.createDataFrame(batch_data, schema)
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
        save_to_postgres(df)
        print(f"✅ Stored {len(batch_data)} records.")
        batch_data = []
    except Exception as e:
        print(f"❌ Error writing batch to PostgreSQL: {e}")

# Main loop
print("📡 Starting Consumer...")

while True:
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz).time()

    start_time = dt_time(16, 30)  # 4:30 PM
    end_time = dt_time(9, 0)      # 9:00 AM (next day)

    if start_time <= now or now <= end_time:
        msg = consumer.poll(timeout=1.0)
        if msg is not None and msg.error() is None:
            try:
                record = json.loads(msg.value().decode("utf-8"))
                if record.get("yahoo_symbol") and record.get("timestamp"):
                    batch_data.append(record)

                if len(batch_data) >= BATCH_SIZE or (time.time() - last_flush) >= 3:
                    process_batch()
                    last_flush = time.time()
            except json.JSONDecodeError:
                print("⚠️ Skipping malformed message")
    else:
        print("🕒 Outside allowed time window. Sleeping for 10 minutes...\n")
        time.sleep(600)
