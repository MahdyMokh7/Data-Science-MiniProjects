import json
import pandas as pd
from confluent_kafka import Consumer

def consume_topic_to_df(
    topic: str,
    broker: str = "localhost:9092",
    group_id: str = "df_loader",
    poll_timeout: float = 1.0,
    max_idle_cycles: int = 20
) -> pd.DataFrame:
    """
    Consume all available messages from the given Kafka topic and return
    a pandas DataFrame of the JSON payloads.
    Stops after `max_idle_cycles` consecutive polls return no messages.
    """
    conf = {
        "bootstrap.servers": broker,
        "group.id": f"{group_id}_{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    consumer.poll(1.0)    # give the group 1 second to rebalance

    ##############
    print("Waiting for assignment…")
    print(" Current assignment:", consumer.assignment())


    records = []
    idle_cycles = 0

    while True:
        msg = consumer.poll(poll_timeout)
        if msg is None:
            idle_cycles += 1
            if idle_cycles >= max_idle_cycles:
                break
            continue
        if msg.error():
            print(f"[{topic}] Error: {msg.error()}")
            continue
        # Reset idle counter when we get a real message
        idle_cycles = 0
        # Parse the JSON value payload
        payload = msg.value().decode("utf-8")
        records.append(json.loads(payload))

    consumer.close()

    # Build DataFrame (or empty DF if no records)
    return pd.json_normalize(records) if records else pd.DataFrame()

# List of topics to load
topics = [
    "darooghe.streamApp",
    "darooghe.realTimeAnalytics",
    "darooghe.fraud_alerts"
]

# Consume each into its own DataFrame
dfs: dict[str, pd.DataFrame] = {}
for topic in topics:
    print(f"Consuming topic: {topic} …")
    df = consume_topic_to_df(topic)
    dfs[topic] = df
    print(f" → Loaded {len(df)} records from {topic}")



print(dfs["darooghe.realTimeAnalytics"].head(5))

# Now `dfs` maps each topic name to its pandas DataFrame
# e.g. dfs["darooghe.streamApp"], dfs["darooghe.realTimeAnalytics"], etc.
