from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import logging
import time
import mysql.connector
import pandas as pd
import joblib
from notifier import EmailNotifier


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
# to show only warnings and errors; For production pipelines:
# logging.basicConfig(level=logging.WARNING)

# to suppress all logs except critical ones; For production pipelines:
# logging.basicConfig(level=logging.CRITICAL)

# During development/debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Load model + threshold bundle
try:
    bundle = joblib.load("fraud_detection_bundle.pkl")
    model = bundle["model"]
    threshold = bundle["threshold"]
    logger.info("✅ Loaded model and threshold from fraud_detection_bundle.pkl")
except Exception as e:
    logger.error(f"❌ Failed to load model bundle: {e}")
    exit(1)

# ✅ Feature dtype mapping based on training data
dtype_mapping = {
    "cc_num": "int64",
    "amt": "float64",
    "gender": "int64",
    "city_pop": "int64",
    "age": "int64",
    "trans_hour": "int64",
    "trans_day_of_week": "int64",
    "trans_weekend": "int64",
    "trans_month": "int64",
    "distance": "float64",
    "avg_amt": "float64",
    "total_transactions": "int64",
    "total_fraud": "int64",
    "fraud_rate": "float64",
    "merchant_encoded": "int64",
    "job_encoded": "int64",
    "category_entertainment": "int64",
    "category_food_dining": "int64",
    "category_gas_transport": "int64",
    "category_grocery_net": "int64",
    "category_grocery_pos": "int64",
    "category_health_fitness": "int64",
    "category_home": "int64",
    "category_kids_pets": "int64",
    "category_misc_net": "int64",
    "category_misc_pos": "int64",
    "category_personal_care": "int64",
    "category_shopping_net": "int64",
    "category_shopping_pos": "int64",
    "category_travel": "int64",
}


# ✅ Kafka Consumer
def create_consumer():
    retries = 10
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id="kafka_consumer_group4",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
            )
            logger.info("✅ Connected to Kafka broker")
            return consumer
        except NoBrokersAvailable:
            logger.warning("⏳ Waiting for Kafka broker...")
            time.sleep(5)
            retries -= 1
    raise Exception("❌ Failed to connect to Kafka broker")


# ✅ MySQL Connection
def connect_to_mysql():
    retries = 10
    while retries > 0:
        try:
            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
            )
            logger.info("✅ Connected to MySQL database")
            return connection
        except mysql.connector.Error as err:
            logger.warning(f"⏳ Waiting for MySQL to be ready: {err}")
            time.sleep(5)
            retries -= 1
    raise Exception("❌ Could not connect to MySQL after multiple attempts.")


# ✅ Create predictions table
def create_prediction_table(connection):
    cursor = connection.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS fraud_predictions (
        trans_num VARCHAR(50) PRIMARY KEY,
        probability FLOAT,
        is_fraud TINYINT(1),
        full_json JSON,
        feedback TINYINT(1) DEFAULT NULL
    );
    """
    cursor.execute(query)
    connection.commit()
    cursor.close()
    logger.info("✅ Ensured fraud_predictions table exists with feedback column.")


# ✅ Store result in MySQL
def insert_prediction(connection, trans_num, prob, is_fraud, full_json):
    cursor = connection.cursor()
    query = """
    INSERT INTO fraud_predictions (trans_num, probability, is_fraud, full_json)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        probability = VALUES(probability),
        is_fraud = VALUES(is_fraud),
        full_json = VALUES(full_json);
    """
    # 🔧 Ensure native Python types
    cursor.execute(
        query, (str(trans_num), float(prob), int(is_fraud), json.dumps(full_json))
    )
    connection.commit()
    cursor.close()
    logger.info(
        f"📥 Stored prediction for {trans_num}: Fraud = {is_fraud}, Prob = {prob:.2f}"
    )


# ✅ Initialize Notifier (use environment vars in production)
notifier = EmailNotifier(
    sender="xyz@gmail.com",
    password="xyz",
    receiver="xyz@gmail.com",
)

# ✅ MAIN LOGIC
consumer = create_consumer()
mysql_connection = connect_to_mysql()
create_prediction_table(mysql_connection)

logger.info("🚀 Starting to consume transactions and run fraud detection...")

# To see full JSON record
# for message in consumer:
#     transaction = message.value
#     logger.info(f"📦 Received transaction: {transaction}")

counter = 0

for message in consumer:
    transaction = message.value
    counter += 1
    # logger.info(f"📥 Received transaction #{counter}")
    logger.info(f"📥 Received transaction #{counter} (ID: {transaction['trans_num']})")

    try:
        df = pd.DataFrame([transaction])
        df = df[model.feature_names_in_]

        # ✅ Safe type conversion
        for col, dtype in dtype_mapping.items():
            df[col] = pd.to_numeric(df[col], errors="raise").astype(dtype)

        prob = model.predict_proba(df)[0][1]
        is_fraud = int(prob >= threshold)

        # 🚨 Trigger alert
        if is_fraud:
            logger.warning(
                f"🚨 FRAUD DETECTED! Transaction: {transaction['trans_num']} | Prob: {prob:.2f}"
            )
            # Add feedback URL to transaction
            transaction["feedback_url"] = (
                f"http://feedback:5000/feedback?trans_num={transaction['trans_num']}&user=xyz@gmail.com"
            )

            notifier.send(transaction, prob)

        # ✅ Store in MySQL
        insert_prediction(
            mysql_connection, transaction["trans_num"], prob, is_fraud, transaction
        )

    except Exception as e:
        logger.error(f"❌ Error during prediction or storage: {e}")

# ✅ Close MySQL on exit
mysql_connection.close()
logger.info("✅ MySQL connection closed.")
