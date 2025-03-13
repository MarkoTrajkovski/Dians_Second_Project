from confluent_kafka import Consumer, KafkaException
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import json

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://localhost:5432/stock_data"  # Update with your database
POSTGRES_PROPERTIES = {
    "user": "postgres",  # Your PostgreSQL username (default is "postgres")
    "password": "HappyFriday@21",  # Replace with your actual PostgreSQL password
    "driver": "org.postgresql.Driver"
}

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Change if using remote Kafka broker
    'group.id': 'spark-group',
    'auto.offset.reset': 'earliest'
}
TOPIC = "stock_topic"


# Create Kafka Consumer
def create_consumer():
    try:
        consumer = Consumer(KAFKA_CONFIG)
        consumer.subscribe([TOPIC])
        return consumer
    except KafkaException as e:
        print(f"Error creating Kafka consumer: {e}")
        exit(1)


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStockConsumer") \
    .config("spark.jars", "C:\PostgreSQL\pgJDBC\postgresql-42.7.2.jar") \
    .getOrCreate()


# Function to store data in PostgreSQL
def save_to_postgres(df: DataFrame):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "stock_prices") \
        .options(**POSTGRES_PROPERTIES) \
        .mode("append") \
        .save()
    print("✅ Data successfully saved to PostgreSQL!")


# Process messages from Kafka continuously
def process_messages(consumer, spark):
    print("📡 Listening for new stock data from Kafka...")
    while True:
        msg = consumer.poll(1.0)  # Wait for new messages
        if msg is None:
            continue  # No new data, keep listening
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            # Convert Kafka message to JSON
            record = json.loads(msg.value().decode("utf-8"))
            print(f"📥 Received: {record}")

            # Convert JSON to Spark DataFrame
            df = spark.createDataFrame([record])

            # Select only necessary columns
            df = df.selectExpr(
                "`Yahoo Symbol` as yahoo_symbol",
                "Open as open_price",
                "High as high_price",
                "Low as low_price",
                "Close as close_price"
            )

            df.show()

            # Save data to PostgreSQL
            save_to_postgres(df)

        except Exception as e:
            print(f"⚠️ Error processing message: {e}")


try:
    consumer = create_consumer()
    process_messages(consumer, spark)
except KeyboardInterrupt:
    print("Consumer shutting down...")
finally:
    consumer.close()