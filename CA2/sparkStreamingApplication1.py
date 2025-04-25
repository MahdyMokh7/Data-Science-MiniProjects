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

# ──────────────────────────────────────────────────────────────────────────────
# 2) Real‑time micro‑batch insights: 1 min windows, with 20s step size
# ──────────────────────────────────────────────────────────────────────────────
WINDOW_SIZE = 1  # m
STEP_SIZE = 20  # s
minute_windowed = (
    json_df
      .withWatermark("event_time", "2 minutes")
      .groupBy(
          window(col("event_time"), f"{WINDOW_SIZE} minute", f"{STEP_SIZE} seconds"),
          col("merchant_category")
      )
      .agg(
          F.count("*").alias("txn_count"),
          F.sum("total_amount").alias("txn_volume")
      )
)

# printing in console 
minute_windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "checkpoint/minute_windowed/console") \
    .start()

# storing in a kafka topic
minute_windowed \
  .select(to_json(struct("window","merchant_category","txn_count","txn_volume")).alias("value")) \
  .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "darooghe.streamApp.minute") \
    .option("checkpointLocation", "checkpoint/minute_windowed/kafka") \
    .outputMode("update") \
    .start()


timer_amount = 60
spark.streams.awaitAnyTermination(timer_amount)
print(f"Streaming application terminated after {timer_amount} seconds.")
spark.stop()