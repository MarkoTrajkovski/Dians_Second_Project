from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from confluent_kafka import Consumer
import psycopg2
import json
import time


POSTGRES_URL = "jdbc:postgresql://localhost:5432/stock_data"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "HappyFriday@21",
    "driver": "org.postgresql.Driver"
}

JDBC_DRIVER_PATH = "file:///C:/PostgreSQL/pgJDBC/postgresql-42.7.2.jar"


spark = SparkSession.builder \
    .appName("KafkaStockConsumer") \
    .config("spark.jars", JDBC_DRIVER_PATH) \
    .getOrCreate()


KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stock-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(["stock_topic"])


schema = StructType([
    StructField("yahoo_symbol", StringType(), True),
    StructField("open_price", FloatType(), True),
    StructField("high_price", FloatType(), True),
    StructField("low_price", FloatType(), True),
    StructField("close_price", FloatType(), True),
    StructField("last_price", FloatType(), True),
    StructField("timestamp", StringType(), True)
])


def ensure_table_exists():
    conn = psycopg2.connect(
        dbname="stock_data",
        user="postgres",
        password="HappyFriday@21",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()


    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        yahoo_symbol VARCHAR(10),
        open_price FLOAT,
        high_price FLOAT,
        low_price FLOAT,
        close_price FLOAT,
        last_price FLOAT,  
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)


    cursor.execute("""
        DO $$ 
        BEGIN 
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stock_prices' AND column_name='last_price') THEN
                ALTER TABLE stock_prices ADD COLUMN last_price FLOAT;
            END IF;
        END $$;
    """)

    conn.commit()
    cursor.close()
    conn.close()

ensure_table_exists()


BATCH_SIZE = 500
batch_data = []

def process_batch():
    global batch_data
    if batch_data:
        try:

            df = spark.createDataFrame(batch_data, schema).dropna()
            df = df.withColumn("timestamp", to_timestamp(col("timestamp")))


            df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", "stock_prices") \
                .options(**POSTGRES_PROPERTIES) \
                .mode("append") \
                .save()

            print(f"Successfully stored {len(batch_data)} messages in PostgreSQL")

        except Exception as e:
            print(f"Error writing batch to PostgreSQL: {e}")

        batch_data = []


try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None and msg.error() is None:
            try:
                value = msg.value().decode("utf-8")
                parsed_data = json.loads(value)


                if parsed_data.get("yahoo_symbol") and parsed_data.get("close_price") is not None:
                    batch_data.append(parsed_data)


                if len(batch_data) >= BATCH_SIZE:
                    process_batch()

            except json.JSONDecodeError:
                print(f"Skipping invalid JSON message: {msg.value()}")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    process_batch()
    consumer.close()
