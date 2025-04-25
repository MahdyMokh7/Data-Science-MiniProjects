import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import from_json, col, window, to_json, struct, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)


# ──────────────────────────────────────────────────────────
# 1) Build spark session + Read from kafka + convert to JSON
# ──────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("RealTimeProcessing_WindowedInsights") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),  # ISO string
    StructField("merchant_category", StringType()),
    StructField("total_amount", DoubleType()),
])

raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe",               "darooghe.transactions")
         .option("startingOffsets",         "latest")
         .option("failOnDataLoss",          "false")
         .load()
)

json_df = (
    raw
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), schema).alias("d"))
      .select("d.*")
      .withColumn("event_time", to_timestamp("timestamp"))
)



# ──────────────────────────────────────────────────────────────
# 2) Daily windows (1‑day)
# ─────────────────────────────────────────────────────────────
daily_windowed = (
    json_df \
      .withWatermark("event_time", "2 days")
      .groupBy(window(col("event_time"), "1 day"))
      .agg(
          F.count("*").alias("daily_txn_count"),
          F.sum("total_amount").alias("daily_txn_volume")
      )
      .select(
          col("window.start").alias("date"),
          "daily_txn_count",
          "daily_txn_volume"
      ) 
)

# printing the result in console
daily_windowed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "checkpoint/daily_windowed/console") \
    .start()

# storing into kafka topic
daily_windowed \
  .select(to_json(struct("date","daily_txn_count","daily_txn_volume")).alias("value")) \
  .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "darooghe.streamApp.daily") \
    .option("checkpointLocation", "checkpoint/daily_windowed/kafka") \
    .outputMode("append") \
    .start()

timer_amount = 60
spark.streams.awaitAnyTermination(timer_amount)
print(f"Streaming application terminated after {timer_amount} seconds.")
spark.stop()