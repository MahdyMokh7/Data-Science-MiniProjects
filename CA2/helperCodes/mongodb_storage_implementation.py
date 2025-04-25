from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os

# === Configuration ===
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "darooghe"
COLLECTION_NAME = "transactions"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# === Step 0: Ensure Indexes for Partitioning Strategy ===
collection.create_index("timestamp")
collection.create_index("merchant_id")
collection.create_index("merchant_category")
collection.create_index("customer_type")
print("Indexes created for timestamp, merchant_id, merchant_category, and customer_type.")


# === Step I: Load Data into MongoDB ===
json_path = "data/transactions.json"

if os.path.exists(json_path):
    transactions = []
    with open(json_path, "r") as f:
        for line in f:
            transactions.append(json.loads(line))


    # Insert only if not already loaded
    if collection.count_documents({}) == 0:
        collection.insert_many(transactions)
        print(f"Inserted {len(transactions)} documents into MongoDB.")
    else:
        print("MongoDB already contains transaction data. Skipping insert.")
else:
    print("transactions.json not found.")

# === Step II: Data Retention Policy ===
# Delete documents older than 24 hours based on timestamp
cutoff_time = datetime.utcnow() - timedelta(hours=24)
deleted = collection.delete_many({
    "timestamp": {"$lt": cutoff_time.isoformat()}
})
print(f"Retention Policy: Deleted {deleted.deleted_count} documents older than 24h.")

# === Step III: Aggregations ===

# Summarized transaction data (daily)
daily_summary = collection.aggregate([
    {
        "$group": {
            "_id": {
                "date": {"$substr": ["$timestamp", 0, 10]},
                "merchant_id": "$merchant_id"
            },
            "total_amount": {"$sum": "$total_amount"},
            "count": {"$sum": 1}
        }
    }
])
print("\n--- Daily Summarized Transactions ---")
for doc in daily_summary:
    print(doc)

# Commission reports (per merchant_category)
commission_summary = collection.aggregate([
    {
        "$group": {
            "_id": "$merchant_category",
            "total_commission": {"$sum": "$commission_amount"}
        }
    }
])
print("\n--- Commission Report per Merchant Category ---")
for doc in commission_summary:
    print(doc)
