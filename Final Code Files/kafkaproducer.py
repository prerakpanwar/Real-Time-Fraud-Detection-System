import csv
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import logging

# Kafka settings
KAFKA_TOPIC = "transaction_data"
KAFKA_BROKER = "kafka:9092"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_producer():
    retries = 10
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("✅ Connected to Kafka broker")
            return producer
        except NoBrokersAvailable:
            logger.warning("⏳ Waiting for Kafka broker to be ready...")
            time.sleep(5)
            retries -= 1
    raise Exception("❌ Failed to connect to Kafka broker after retries")


def stream_csv_data(file_path):
    producer = create_producer()
    total_rows = 0
    start_time = time.time()

    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)

            for row in reader:
                producer.send(KAFKA_TOPIC, row)
                producer.flush()
                total_rows += 1
                # logger.info(f"📤 Streamed record #{total_rows}")          # Just to get count

                # TO GET FULL JSON RECORD
                # logger.info(f"📤 Streaming transaction #{total_rows}: {row}")

                trans_id = row.get("trans_num", "N/A")
                logger.info(f"📤 Streamed transaction #{total_rows} (ID: {trans_id})")

                time.sleep(0.2)  # Simulate real-time delay

        producer.close()
        elapsed = time.time() - start_time
        logger.info("✅ Data streaming complete.")
        logger.info("Total records streamed: %s", total_rows)
        logger.info("⏱️ Time taken: %.2f seconds", elapsed)
        logger.info("Average throughput: %.2f records/second", total_rows / elapsed)

    except Exception as e:
        logger.error(f"❌ Error reading CSV or sending data to Kafka: {e}")
        producer.close()


# Run the function with your CSV
stream_csv_data("final_transactions.csv")


# FOR BATCH LOADS
# import csv
# import time
# from kafka import KafkaProducer
# from kafka.errors import NoBrokersAvailable
# import json
# import logging

# # Kafka settings
# KAFKA_TOPIC = "transaction_data"
# KAFKA_BROKER = "kafka:9092"

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Batch size
# BATCH_SIZE = 50  # Tune as needed


# # Retry mechanism to wait for Kafka broker
# def create_producer():
#     retries = 10
#     while retries > 0:
#         try:
#             producer = KafkaProducer(
#                 bootstrap_servers=KAFKA_BROKER,
#                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#             )
#             logger.info("✅ Connected to Kafka broker")
#             return producer
#         except NoBrokersAvailable:
#             logger.warning("⏳ Waiting for Kafka broker to be ready...")
#             time.sleep(5)
#             retries -= 1
#     raise Exception("❌ Failed to connect to Kafka broker after retries")


# # Function to stream CSV data in batches
# def stream_csv_data(file_path):
#     producer = create_producer()
#     total_rows = 0
#     start_time = time.time()  # ⏱ Start timer

#     try:
#         with open(file_path, "r") as file:
#             reader = csv.DictReader(file)
#             batch = []

#             for row in reader:
#                 batch.append(row)
#                 total_rows += 1
#                 if len(batch) == BATCH_SIZE:
#                     for record in batch:
#                         producer.send(KAFKA_TOPIC, record)
#                     producer.flush()
#                     batch = []

#             # Send any remaining records
#             if batch:
#                 for record in batch:
#                     producer.send(KAFKA_TOPIC, record)
#                 producer.flush()

#         producer.close()
#         elapsed = time.time() - start_time  # ⏱ End timer

#         logger.info("✅ Data streaming complete.")
#         logger.info("📊 Total records streamed: %s", total_rows)
#         logger.info("⏱️ Time taken: %.2f seconds", elapsed)
#         logger.info("🚀 Average throughput: %.2f records/second", total_rows / elapsed)

#     except Exception as e:
#         logger.error(f"❌ Error reading CSV or sending data to Kafka: {e}")
#         producer.close()


# # Run the function with your CSV
# stream_csv_data("final_transactions.csv")
