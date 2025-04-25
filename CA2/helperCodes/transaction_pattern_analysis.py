from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    hour, to_date, dayofweek, col, count, sum, avg,
    window, expr, when, max, min
)

# Step 1: Create Spark session
spark = SparkSession.builder.appName("TransactionPatternAnalysis").getOrCreate()

# Step 2: Load data
df = spark.read.json("data/transactions.json")

# Step 3: Preprocessing - extract useful time features
df = df.withColumn("transaction_date", to_date("timestamp")) \
       .withColumn("transaction_hour", hour("timestamp")) \
       .withColumn("day_of_week", dayofweek("timestamp"))

# === I. Discover temporal patterns (daily trend) ===
daily_trend = df.groupBy("transaction_date") \
    .agg(count("*").alias("daily_transactions"))\
    .orderBy("transaction_date")

# === II. Identify peak transaction hours ===
hourly_trend = df.groupBy("transaction_hour") \
    .agg(count("*").alias("transaction_count")) \
    .orderBy("transaction_hour")

# === III. Segment customers by spending frequency ===
customer_segments = df.groupBy("customer_id") \
    .agg(
        count("*").alias("transaction_count"),
        sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_spent")
    ) \
    .orderBy(col("transaction_count").desc())

# === IV. Compare transaction behavior across merchant categories ===
merchant_behavior = df.groupBy("merchant_category") \
    .agg(
        count("*").alias("transaction_count"),
        sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_transaction_amount")
    ) \
    .orderBy(col("transaction_count").desc())

# === V. Identify morning/afternoon/evening transaction patterns ===
df = df.withColumn(
    "time_of_day",
    when(col("transaction_hour") < 12, "morning")
    .when((col("transaction_hour") >= 12) & (col("transaction_hour") < 18), "afternoon")
    .otherwise("evening")
)

time_of_day_pattern = df.groupBy("time_of_day") \
    .agg(count("*").alias("transaction_count"))

# === VI. Spending trend over time (is it increasing?) ===
spending_trend = df.groupBy("transaction_date") \
    .agg(
        sum("total_amount").alias("total_spent"),
        max("total_amount").alias("max_spent"),
        min("total_amount").alias("min_spent"),
        avg("total_amount").alias("avg_spent")
    ) \
    .orderBy("transaction_date")

# === Show results ===
print("=== Daily Transaction Trend ===")
daily_trend.show(truncate=False)

print("=== Hourly Peak Times ===")
hourly_trend.show(truncate=False)

print("=== Customer Segments ===")
customer_segments.show(truncate=False)

print("=== Merchant Behavior ===")
merchant_behavior.show(truncate=False)

print("=== Time of Day Activity ===")
time_of_day_pattern.show(truncate=False)

print("=== Spending Over Time ===")
spending_trend.show(truncate=False)

# === Optionally: Save results to folders ===
daily_trend.write.mode("overwrite").json("output/TransactionPatterns/daily_trend")
hourly_trend.write.mode("overwrite").json("output/TransactionPatterns/hourly_peak")
customer_segments.write.mode("overwrite").json("output/TransactionPatterns/customer_segments")
merchant_behavior.write.mode("overwrite").json("output/TransactionPatterns/merchant_behavior")
time_of_day_pattern.write.mode("overwrite").json("output/TransactionPatterns/time_of_day")
spending_trend.write.mode("overwrite").json("output/TransactionPatterns/spending_trend")
