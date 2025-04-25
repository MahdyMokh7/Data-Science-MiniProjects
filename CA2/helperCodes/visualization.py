###############  PREPARING ENVIROMENT  ################

import os
import json
import pandas as pd

def load_json_to_df(path: str) -> pd.DataFrame:
    """
    ----------
    path : str
        Full filesystem path to the .json file.
    
    Returns
    -------
    pd.DataFrame
    """
    records = []
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # parse each JSON object per line
            records.append(json.loads(line))
    # normalize (flattens nested structures if any)
    return pd.json_normalize(records)

output_dir = 'output'
dfs_dict = {}

for dirpath, dirnames, filenames in os.walk(output_dir):
    for fn in filenames:
        if not fn.endswith('.json'):
            continue
        full_path = os.path.join(dirpath, fn)
        rel_path = os.path.relpath(full_path, output_dir)
        parts = rel_path.split(os.sep)

        # TransactionPatterns/<patternName>/<patternName>.json  → patterns_<patternName>
        if parts[0] == 'TransactionPatterns' and len(parts) == 3:
            pattern = parts[1]
            key = f'{parts[0]}_{pattern}'
        
        # CommissionAnalysis/<commissionName>/<patternName>.json  → patterns_<patternName>
        elif parts[0] == 'CommissionAnalysis' and len(parts) == 3:
            key = f'{parts[0]}_{parts[1]}'

        
        # Direct JSON file in output/  → <filename>
        else:
            key = os.path.splitext(parts[0])[0]
        
        print(full_path)
        dfs_dict[key] = load_json_to_df(full_path)



# print(dfs_dict.keys())
# print(dfs_dict['TransactionPatterns_daily_trend'].info())
# print('------------------------------------------------')
# print(dfs_dict['TransactionPatterns_daily_trend'].describe())
# print('------------------------------------------------')
# print(dfs_dict['TransactionPatterns_daily_trend'].shape)


for key, value in dfs_dict.items():
    print(f"key:  {key}")
    print(value.head(5))
    print('-----------------------------------------')


# Now `dfs_dict` is a dict mapping keys → DataFrames.
# e.g. dfs_dict['patterns_pattern1'], dfs_dict['batch_transactions'], dfs_dict['somefile'], etc.


import json
import pandas as pd
from confluent_kafka import Consumer

def consume_topic_to_df(
    topic: str,
    broker: str = "localhost:9092",
    group_id: str = "df_loader",
    poll_timeout: float = 1.0,
    max_idle_cycles: int = 10
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



# print(dfs["darooghe.realTimeAnalytics"].head(5))

# Now `dfs` maps each topic name to its pandas DataFrame
# e.g. dfs["darooghe.streamApp"], dfs["darooghe.realTimeAnalytics"], etc.



for key, value in dfs.items():
    print(f"key:  {key}")
    print(value.head(5))
    print('-----------------------------------------')




#############################################################
# ------------------------------------------------
#############################################################




import pandas as pd
import matplotlib.pyplot as plt

# 1) Prepare your historical daily‐volume DataFrame
#    (you called this TransactionPatterns_daily_trend)
hist = dfs_dict["TransactionPatterns_daily_trend"].copy()
hist["transaction_date"] = pd.to_datetime(hist["transaction_date"])
hist = hist.sort_values("transaction_date")

# 2) Prepare your real‐time volume DataFrame
#    (you called this darooghe.streamApp)
rt = dfs["darooghe.streamApp"].copy()
rt["window.start"] = pd.to_datetime(rt["window.start"])
# aggregate across merchants to get total transactions per window
rt_agg = (
    rt
    .groupby("window.start")["count"]
    .sum()
    .reset_index()
    .rename(columns={"count": "real_time_txn_count"})
)

# 3) Plot them together
plt.figure(figsize=(12, 6))

# historical: daily
plt.plot(
    hist["transaction_date"],
    hist["daily_transactions"],
    marker="o",
    linestyle="-",
    label="Historical (daily)"
)

# real‑time: per‐minute
plt.plot(
    rt_agg["window.start"],
    rt_agg["real_time_txn_count"],
    marker=".",
    linestyle="--",
    label="Real‑time (per minute)"
)

plt.title("Transaction Volume: Historical vs Real‑Time")
plt.xlabel("Timestamp")
plt.ylabel("Number of Transactions")
plt.legend()
plt.tight_layout()
plt.show()
