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


# Retry mechanism to wait for Kafka broker
def create_producer():
    retries = 10
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("Connected to Kafka broker")
            return producer
        except NoBrokersAvailable:
            logger.warning("Waiting for Kafka broker to be ready...")
            time.sleep(5)
            retries -= 1
    raise Exception("Failed to connect to Kafka broker after retries")


# Function to stream CSV data
def stream_csv_data(file_path):
    producer = create_producer()
    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Remove PII and unnecessary columns
                filtered_row = {
                    "trans_num": row["trans_num"],  # Unique transaction ID
                    "trans_date_trans_time": row[
                        "trans_date_trans_time"
                    ],  # Transaction timestamp
                    "cc_num": row[
                        "cc_num"
                    ],  # Credit card number (masked or hashed in production)
                    "merchant": row["merchant"],  # Merchant name
                    "category": row["category"],  # Transaction category
                    "amt": float(row["amt"]),  # Transaction amount
                    "lat": float(row["lat"]),  # Latitude of cardholder
                    "long": float(row["long"]),  # Longitude of cardholder
                    "city_pop": int(row["city_pop"]),  # City population
                    "job": row["job"],  # Job title
                    "merch_lat": float(row["merch_lat"]),  # Merchant latitude
                    "merch_long": float(row["merch_long"]),  # Merchant longitude
                    "is_fraud": int(row["is_fraud"]),  # Fraud flag (0 or 1)
                }
                # Send the filtered row to Kafka
                producer.send(KAFKA_TOPIC, filtered_row)
                logger.info(f"Sent: {filtered_row}")
                # Remove or reduce the sleep time for faster streaming
                # time.sleep(1)  # Simulating real-time data stream

        producer.flush()
        producer.close()
        logger.info("Data streaming complete.")
    except Exception as e:
        logger.error(f"Error reading CSV or sending data to Kafka: {e}")
        producer.close()


# Call function with your CSV file
stream_csv_data("docker_train_sample2_copy.csv")


# # WITHOUT MASKING
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


# # Retry mechanism to wait for Kafka broker
# def create_producer():
#     retries = 10
#     while retries > 0:
#         try:
#             producer = KafkaProducer(
#                 bootstrap_servers=KAFKA_BROKER,
#                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#             )
#             logger.info("Connected to Kafka broker")
#             return producer
#         except NoBrokersAvailable:
#             logger.warning("Waiting for Kafka broker to be ready...")
#             time.sleep(5)
#             retries -= 1
#     raise Exception("Failed to connect to Kafka broker after retries")


# # Function to stream CSV data
# def stream_csv_data(file_path):
#     producer = create_producer()
#     try:
#         with open(file_path, "r") as file:
#             reader = csv.DictReader(file)
#             for row in reader:
#                 # Remove PII and keep necessary fields
#                 filtered_row = {
#                     "trans_num": row["trans_num"],  # Primary key
#                     "trans_date_trans_time": row["trans_date_trans_time"],
#                     "merchant": row["merchant"],
#                     "category": row["category"],
#                     "amt": row["amt"],
#                     "state": row["state"],
#                     "lat": row["lat"],  # Unmasked latitude
#                     "long": row["long"],  # Unmasked longitude
#                     "city_pop": row["city_pop"],
#                     "job": row["job"],
#                     "merch_lat": row["merch_lat"],  # Unmasked merchant latitude
#                     "merch_long": row["merch_long"],  # Unmasked merchant longitude
#                     "is_fraud": row["is_fraud"],
#                 }
#                 # Send the filtered row to Kafka
#                 producer.send(KAFKA_TOPIC, filtered_row)
#                 logger.info(f"Sent: {filtered_row}")
#                 # Remove or reduce the sleep time for faster streaming
#                 # time.sleep(1)  # Simulating real-time data stream

#         producer.flush()
#         producer.close()
#         logger.info("Data streaming complete.")
#     except Exception as e:
#         logger.error(f"Error reading CSV or sending data to Kafka: {e}")
#         producer.close()


# # Call function with your CSV file
# stream_csv_data("docker_train_sample2.csv")

# REAL CODE:

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


# # Retry mechanism to wait for Kafka broker
# def create_producer():
#     retries = 10
#     while retries > 0:
#         try:
#             producer = KafkaProducer(
#                 bootstrap_servers=KAFKA_BROKER,
#                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#             )
#             logger.info("Connected to Kafka broker")
#             return producer
#         except NoBrokersAvailable:
#             logger.warning("Waiting for Kafka broker to be ready...")
#             time.sleep(5)
#             retries -= 1
#     raise Exception("Failed to connect to Kafka broker after retries")


# # Function to stream CSV data
# def stream_csv_data(file_path):
#     producer = create_producer()
#     with open(file_path, "r") as file:
#         reader = csv.DictReader(file)
#         for row in reader:
#             producer.send(KAFKA_TOPIC, row)
#             logger.info(f"Sent: {row}")
#             time.sleep(1)  # Simulating real-time data stream

#     producer.flush()
#     producer.close()
#     logger.info("Data streaming complete.")


# # Call function with your CSV file
# stream_csv_data("book.csv")
