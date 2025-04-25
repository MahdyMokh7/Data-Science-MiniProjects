import os
import time
import random
import uuid
import json
import datetime
import logging
from datetime import timedelta
from confluent_kafka import Producer, Consumer, TopicPartition  ##### Kafka Producer, Consumer, and TopicPartition classes
from confluent_kafka.admin import AdminClient  ##### Kafka AdminClient for topic management

# Logging setup
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level_str, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)

# Constants and data pools for event generation
MERCHANT_CATEGORIES = [
    "retail",
    "food_service",
    "entertainment",
    "transportation",
    "government",
]
PAYMENT_METHODS = ["online", "pos", "mobile", "nfc"]
COMMISSION_TYPES = ["flat", "progressive", "tiered"]
CUSTOMER_TYPES = ["individual", "CIP", "business"]
FAILURE_REASONS = ["cancelled", "insufficient_funds", "system_error", "fraud_prevented"]
DEVICE_INFO_LIBRARY = [
    {"os": "Android", "app_version": "2.4.1", "device_model": "Samsung Galaxy S25"},
    {"os": "iOS", "app_version": "3.1.0", "device_model": "iPhone 15"},
    {"os": "Android", "app_version": "1.9.5", "device_model": "Google Pixel 6"},
]

# Utility function to generate a random datetime between two points
def generate_random_datetime(start, end):
    delta = end - start
    random_seconds = random.uniform(0, delta.total_seconds())
    return start + timedelta(seconds=random_seconds)

# Generates a synthetic transaction event
def generate_transaction_event(is_historical=False, timestamp_override=None):
    event_time = (
        timestamp_override if timestamp_override else datetime.datetime.utcnow()
    )
    transaction_id = str(uuid.uuid4())
    customer_id = f"cust_{random.randint(1, customer_count)}"
    merchant_id = f"merch_{random.randint(1, merchant_count)}"
    merchant_category = random.choice(MERCHANT_CATEGORIES)
    payment_method = random.choice(PAYMENT_METHODS)
    amount = random.randint(50000, 2000000)
    base_lat = 35.7219
    base_lng = 51.3347
    location = {
        "lat": base_lat + random.uniform(-0.05, 0.05),
        "lng": base_lng + random.uniform(-0.05, 0.05),
    }
    device_info = (
        random.choice(DEVICE_INFO_LIBRARY)
        if payment_method in ["online", "mobile"]
        else {}
    )
    if random.random() < declined_rate:
        status = "declined"
        failure_reason = random.choice(FAILURE_REASONS)
    else:
        status = "approved"
        failure_reason = None
    risk_level = 5 if random.random() < fraud_rate else random.randint(1, 3)
    commission_type = random.choice(COMMISSION_TYPES)
    commission_amount = int(amount * 0.02)
    vat_amount = int(amount * 0.09)
    total_amount = amount + vat_amount
    event = {
        "transaction_id": transaction_id,
        "timestamp": event_time.isoformat() + "Z",
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "merchant_category": merchant_category,
        "payment_method": payment_method,
        "amount": amount,
        "location": location,
        "device_info": device_info,
        "status": status,
        "commission_type": commission_type,
        "commission_amount": commission_amount,
        "vat_amount": vat_amount,
        "total_amount": total_amount,
        "customer_type": random.choice(CUSTOMER_TYPES),
        "risk_level": risk_level,
        "failure_reason": failure_reason,
    }
    return event

# Callback function to log the result of message delivery
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")

# Generates and sends historical events to Kafka
def produce_historical_events(producer, topic, count=20000):
    logging.info(f"Producing {count} historical events...")
    now = datetime.datetime.utcnow()
    start_time = now - timedelta(days=7)
    for _ in range(count):
        event_time = generate_random_datetime(start_time, now)
        event = generate_transaction_event(timestamp_override=event_time)
        producer.produce(  ##### Send message to Kafka topic
            topic,  ##### Kafka topic name
            key=event["customer_id"],  ##### Key used for Kafka partitioning
            value=json.dumps(event),  ##### Message payload (JSON string)
            callback=delivery_report,  ##### Callback to report delivery status
        )
    producer.flush()  ##### Ensure all messages are sent before proceeding
    logging.info("Historical events production completed.")

# Simulates real-time transaction streaming into Kafka
def continuous_event_production(producer, topic, base_rate):
    while True:
        current_hour = datetime.datetime.utcnow().hour
        multiplier = peak_factor if 9 <= current_hour < 18 else 1.0
        effective_rate = base_rate * multiplier
        lambda_per_sec = effective_rate / 60.0
        wait_time = random.expovariate(lambda_per_sec)
        time.sleep(wait_time)
        event = generate_transaction_event()
        producer.produce(  ##### Send real-time transaction to Kafka
            topic,  ##### Kafka topic name
            key=event["customer_id"],  ##### Partitioning key
            value=json.dumps(event),  ##### Transaction payload
            callback=delivery_report,  ##### Callback for delivery confirmation
        )
        producer.poll(0)  ##### Trigger callbacks for delivery report

# Deletes a Kafka topic (flush/reset) using AdminClient
def flush_topic(broker, topic):
    admin_client = AdminClient({"bootstrap.servers": broker})  ##### Kafka AdminClient initialized with broker
    topics = admin_client.list_topics(timeout=10).topics  ##### Fetch current topic list from Kafka
    if topic in topics:
        fs = admin_client.delete_topics([topic], operation_timeout=30)  ##### Request Kafka to delete the topic
        for t, f in fs.items():
            try:
                f.result()
                logging.info(f"Topic {t} deleted")
            except Exception as e:
                logging.error(f"Deletion failed for topic {t}: {e}")
        time.sleep(10)

# Check if a Kafka topic already has messages
def topic_has_messages(broker, topic):
    conf_cons = {
        "bootstrap.servers": broker,  ##### Kafka broker connection for temporary consumer
        "group.id": "dummy",  ##### Dummy consumer group ID
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf_cons)  ##### Create a Kafka consumer to inspect topic
    tp = TopicPartition(topic, 0)  ##### Check the first partition of the topic
    try:
        low, high = consumer.get_watermark_offsets(tp)  ##### Get earliest and latest offsets (message range)
        return high > low
    except Exception:
        return False
    finally:
        consumer.close()  ##### Close the temporary consumer

# Main entry point
if __name__ == "__main__":
    EVENT_RATE = float(os.getenv("EVENT_RATE", 100))
    peak_factor = float(os.getenv("PEAK_FACTOR", 2.5))
    fraud_rate = float(os.getenv("FRAUD_RATE", 0.02))
    declined_rate = float(os.getenv("DECLINED_RATE", 0.05))
    merchant_count = int(os.getenv("MERCHANT_COUNT", 50))
    customer_count = int(os.getenv("CUSTOMER_COUNT", 1000))

    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")  ##### Kafka broker address (default is localhost:9092)
    topic = "darooghe.transactions"  ##### We will send the data (msgs) to this Kafka topic

    event_init_mode = os.getenv("EVENT_INIT_MODE", "flush").lower()  ##### Determines topic startup behavior: flush or skip
    skip_initial = False

    if event_init_mode == "flush":
        flush_topic(kafka_broker, topic)  ##### Clear existing topic data if flush mode is set
    elif event_init_mode == "skip":
        if topic_has_messages(kafka_broker, topic):  ##### Check if topic already has data
            logging.info("Topic has messages; skipping historical events production.")
            skip_initial = True

    conf = {"bootstrap.servers": kafka_broker}  ##### Kafka producer configuration using the broker address
    producer = Producer(conf)  ##### Initialize Kafka producer using the config

    if not skip_initial:
        produce_historical_events(producer, topic, count=20000)  ##### Produce historical messages if not skipping

    logging.info("Starting continuous event production...")
    continuous_event_production(producer, topic, base_rate=EVENT_RATE)  ##### Begin streaming real-time transaction events to Kafka
