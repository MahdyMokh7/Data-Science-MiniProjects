import os
import json
import logging
from confluent_kafka import Consumer
from helperCodes.consumer import Transaction  # existing Transaction class

log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level_str, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)

kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
topic = "darooghe.transactions"
group_id = "json-dump-consumer-group"

conf = {
    "bootstrap.servers": kafka_broker,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
}

OUTPUT_DIR = "./data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_PATH = "data/transactions.json"
MAX_TRANSACTIONS = 20000  # number of historical transactions to save

if __name__ == "__main__":
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print(f"Dumping up to {MAX_TRANSACTIONS} historical transactions to JSON...")

    count = 0
    transactions = []

    try:
    
        while count < MAX_TRANSACTIONS:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            transaction_data = json.loads(msg.value().decode("utf-8"))

            try:
                transaction = Transaction(transaction_data)
                transactions.append(transaction)
                count += 1
            except Exception as e:
                continue

        with open(OUTPUT_PATH, "w") as outfile:
            for t in transactions:
                json.dump(t.__dict__, outfile)
                outfile.write("\n")



    finally:
        consumer.close()
        print(f"Finished dumping {count} transactions.")
        print(len(transactions))
