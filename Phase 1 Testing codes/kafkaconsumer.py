from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import logging
import time
import mysql.connector

# Kafka settings
KAFKA_TOPIC = "transaction_data"
KAFKA_BROKER = "kafka:9092"

# MySQL settings
MYSQL_HOST = "mysql"
MYSQL_PORT = 3307
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "kafka_data"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Function to create Kafka Consumer
def create_consumer():
    retries = 10
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id="kafka_consumer_group",  # ✅ Ensure consumer belongs to this group
                enable_auto_commit=True,  # ✅ Ensures offsets are committed automatically
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",  # ✅ Ensures all messages are read from the beginning
            )
            logger.info("Connected to Kafka broker")
            return consumer
        except NoBrokersAvailable:
            logger.warning("Waiting for Kafka broker to be ready...")
            time.sleep(5)
            retries -= 1
    raise Exception("Failed to connect to Kafka broker after retries")


# Function to connect to MySQL
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
        logger.info("Connected to MySQL database")
        return connection
    except mysql.connector.Error as err:
        logger.error(f"Error connecting to MySQL: {err}")
        raise


# Function to create table if it does not exist
def create_table_if_not_exists(connection):
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS transactions (
            trans_num VARCHAR(50) PRIMARY KEY,
            trans_date_trans_time DATETIME NOT NULL,
            cc_num BIGINT NOT NULL,
            merchant VARCHAR(255) NOT NULL,
            category VARCHAR(100) NOT NULL,
            amt DECIMAL(10,2) NOT NULL,
            lat DECIMAL(9,6),
            `long` DECIMAL(9,6),
            city_pop INT,
            job VARCHAR(255),
            merch_lat DECIMAL(9,6),
            merch_long DECIMAL(9,6),
            is_fraud TINYINT(1) NOT NULL
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        logger.info("Checked and ensured that the transactions table exists.")
    except mysql.connector.Error as err:
        logger.error(f"Failed to create table: {err}")
        raise


# Function to insert data into MySQL
def insert_into_mysql(connection, transaction):
    try:
        cursor = connection.cursor()
        query = """
            INSERT IGNORE INTO transactions (
                trans_num, trans_date_trans_time, cc_num, merchant, category, 
                amt, lat, `long`, city_pop, job, merch_lat, merch_long, is_fraud
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            transaction["trans_num"],
            transaction["trans_date_trans_time"],
            transaction["cc_num"],
            transaction["merchant"],
            transaction["category"],
            transaction["amt"],
            transaction["lat"],
            transaction["long"],
            transaction["city_pop"],
            transaction["job"],
            transaction["merch_lat"],
            transaction["merch_long"],
            transaction["is_fraud"],
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()
        logger.info(f"✅ Inserted into MySQL: {transaction}")
    except mysql.connector.Error as err:
        logger.error(f"❌ Failed to insert into MySQL: {err}, Data: {transaction}")


# Create Kafka Consumer
consumer = create_consumer()

# Connect to MySQL
mysql_connection = connect_to_mysql()

# Ensure the table exists before consuming messages
create_table_if_not_exists(mysql_connection)

# Start consuming messages
logger.info("Starting to consume messages...")
for message in consumer:
    transaction = message.value
    logger.info(f"Received transaction: {transaction}")

    # Store fraud transactions only
    # if transaction["is_fraud"] == 0:
    insert_into_mysql(mysql_connection, transaction)

# Close MySQL connection when stopped
mysql_connection.close()
logger.info("MySQL connection closed.")


# from kafka import KafkaConsumer
# from kafka.errors import NoBrokersAvailable
# import json
# import logging
# import time
# import mysql.connector

# # Kafka settings
# KAFKA_TOPIC = "transaction_data"
# KAFKA_BROKER = "kafka:9092"

# # MySQL settings
# MYSQL_HOST = "mysql"
# MYSQL_PORT = 3307
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "password"
# MYSQL_DATABASE = "kafka_data"

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# # Function to create Kafka Consumer
# def create_consumer():
#     retries = 10
#     while retries > 0:
#         try:
#             consumer = KafkaConsumer(
#                 KAFKA_TOPIC,
#                 bootstrap_servers=KAFKA_BROKER,
#                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#                 auto_offset_reset="latest",
#             )
#             logger.info("Connected to Kafka broker")
#             return consumer
#         except NoBrokersAvailable:
#             logger.warning("Waiting for Kafka broker to be ready...")
#             time.sleep(5)
#             retries -= 1
#     raise Exception("Failed to connect to Kafka broker after retries")


# # Function to connect to MySQL
# def connect_to_mysql():
#     try:
#         connection = mysql.connector.connect(
#             host=MYSQL_HOST,
#             user=MYSQL_USER,
#             password=MYSQL_PASSWORD,
#             database=MYSQL_DATABASE,
#         )
#         logger.info("Connected to MySQL database")
#         return connection
#     except mysql.connector.Error as err:
#         logger.error(f"Error connecting to MySQL: {err}")
#         raise


# # Function to insert data into MySQL
# def insert_into_mysql(connection, transaction):
#     try:
#         cursor = connection.cursor()
#         query = """
#             INSERT IGNORE INTO transactions (
#                 trans_num, trans_date_trans_time, cc_num, merchant, category,
#                 amt, lat, long, city_pop, job, merch_lat, merch_long, is_fraud
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """
#         values = (
#             transaction["trans_num"],
#             transaction["trans_date_trans_time"],
#             transaction["cc_num"],
#             transaction["merchant"],
#             transaction["category"],
#             transaction["amt"],
#             transaction["lat"],
#             transaction["long"],
#             transaction["city_pop"],
#             transaction["job"],
#             transaction["merch_lat"],
#             transaction["merch_long"],
#             transaction["is_fraud"],
#         )
#         cursor.execute(query, values)
#         connection.commit()
#         cursor.close()
#         logger.info(f"Inserted into MySQL: {transaction}")
#     except mysql.connector.Error as err:
#         logger.error(f"Failed to insert into MySQL: {err}")


# # Create Kafka Consumer
# consumer = create_consumer()

# # Connect to MySQL
# mysql_connection = connect_to_mysql()

# # Start consuming messages
# logger.info("Starting to consume messages...")
# for message in consumer:
#     transaction = message.value
#     logger.info(f"Received transaction: {transaction}")

#     # Store fraud transactions only
#     if transaction["is_fraud"] == 1:
#         insert_into_mysql(mysql_connection, transaction)

# # Close MySQL connection when stopped
# mysql_connection.close()
# logger.info("MySQL connection closed.")
