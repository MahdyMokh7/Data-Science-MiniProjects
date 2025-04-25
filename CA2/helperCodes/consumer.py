import os
import json
import logging
from confluent_kafka import Consumer, Producer
from datetime import datetime
from typing import Dict

class Transaction:
    def __init__(self, data: dict):
        self.transaction_id = str(data.get("transaction_id"))
        self.timestamp = str(data.get("timestamp"))
        self.customer_id = str(data.get("customer_id"))
        self.merchant_id = str(data.get("merchant_id"))
        self.merchant_category = str(data.get("merchant_category"))
        self.payment_method = str(data.get("payment_method"))
        self.amount = int(data.get("amount"))
        self.location = dict(data.get("location", {}))
        self.device_info = dict(data.get("device_info", {}))
        self.status = str(data.get("status"))
        self.commission_type = str(data.get("commission_type"))
        self.commission_amount = int(data.get("commission_amount"))
        self.vat_amount = int(data.get("vat_amount"))
        self.total_amount = int(data.get("total_amount"))
        self.customer_type = str(data.get("customer_type"))
        self.risk_level = int(data.get("risk_level"))
        self.failure_reason = data.get("failure_reason")

    ##### Rule 1 - Amount Consistency
    def validate_amount_consistency(self, producer: Producer, raw_data: Dict):
        if self.total_amount != self.amount + self.vat_amount + self.commission_amount:
            error_payload = {
                "transaction_id": self.transaction_id,
                "error_code": "ERR_AMOUNT",
                "error_message": "Total amount mismatch",
                "original_data": raw_data
            }
            producer.produce("darooghe.error_logs", key=self.transaction_id, value=json.dumps(error_payload))

    ##### Rule 2 - Time Warping
    def validate_time_warping(self, producer: Producer, raw_data: Dict):
        try:
            event_time = datetime.strptime(self.timestamp, "%Y-%m-%dT%H:%M:%S")  ##### Adjust format if needed
            now = datetime.utcnow()
            if event_time > now or (now - event_time).days > 1:
                error_payload = {
                    "transaction_id": self.transaction_id,
                    "error_code": "ERR_TIME",
                    "error_message": "Timestamp is either from the future or too old",
                    "original_data": raw_data
                }
                producer.produce("darooghe.error_logs", key=self.transaction_id, value=json.dumps(error_payload))
        except Exception as e:
            logging.warning(f"Time parsing error: {e}")

    ##### Rule 3 - Device Mismatch
    def validate_device_mismatch(self, producer: Producer, raw_data: Dict):
        if self.payment_method.lower() == "mobile":
            os_name = self.device_info.get("os", "").lower()
            if os_name not in ["ios", "android"]:
                error_payload = {
                    "transaction_id": self.transaction_id,
                    "error_code": "ERR_DEVICE",
                    "error_message": "Unexpected device OS for mobile payment",
                    "original_data": raw_data
                }
                producer.produce("darooghe.error_logs", key=self.transaction_id, value=json.dumps(error_payload))


log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level_str, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)

kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")  ##### Address of the Kafka broker (localhost or remote)
topic = "darooghe.transactions"  ##### Kafka topic to which the producer sends data and from which the consumer reads
error_topic = "darooghe.error_logs"
group_id = "darooghe-consumer-group"  ##### Unique ID to identify this group of consumers (helps Kafka track offsets)

# Kafka consumer configuration dictionary
conf = {
    "bootstrap.servers": kafka_broker,  ##### Kafka broker address for the consumer to connect
    "group.id": group_id,  ##### Assign consumer to a specific group ID
    "auto.offset.reset": "earliest",  ##### If no previous offset, start from the beginning of the topic
    "enable.auto.commit": True  ##### Automatically commit the offset after consuming each message
}

if __name__ == "__main__":

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    producer = Producer({"bootstrap.servers": kafka_broker})

    print("Consumer is now listening to topic:", topic)

    try:
        while True:
            print("wait(Mehdy)..")
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            transaction_data = json.loads(msg.value().decode("utf-8"))

            try:
                transaction = Transaction(transaction_data)
                print("Valid Transaction Object:")
                print(vars(transaction))

                ##### Run validations on the transaction
                transaction.validate_amount_consistency(producer, transaction_data)
                transaction.validate_time_warping(producer, transaction_data)
                transaction.validate_device_mismatch(producer, transaction_data)

                producer.flush()

            except Exception as e:
                error_payload = {
                    "transaction_id": transaction_data.get("transaction_id"),
                    "error_code": "ERR_SCHEMA",
                    "error_message": str(e),
                    "original_data": transaction_data
                }
                producer.produce(error_topic, key=transaction_data.get("transaction_id"), value=json.dumps(error_payload))
                producer.flush()
            break

    finally:
        consumer.close()
