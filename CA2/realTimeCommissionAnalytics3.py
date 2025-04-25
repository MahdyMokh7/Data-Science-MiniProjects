import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, to_json, struct, sum as _sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

# -----------------------------------------------------------
# 1) Build spark session + Read from kafka + convert to JSON
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RealTimeProcessing_CommissionAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("payment_method", StringType()),
    StructField("amount", IntegerType()),
    StructField("location", StructType([
        StructField("lat", DoubleType()),
        StructField("lng", DoubleType())
    ])),
    StructField("device_info", StructType([
        StructField("os", StringType()),
        StructField("app_version", StringType()),
        StructField("device_model", StringType())
    ])),
    StructField("status", StringType()),
    StructField("commission_type", StringType()),
    StructField("commission_amount", IntegerType()),
    StructField("vat_amount", IntegerType()),
    StructField("total_amount", IntegerType()),
    StructField("customer_type", StringType()),
    StructField("risk_level", IntegerType()),
    StructField("failure_reason", StringType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe",               "darooghe.transactions") \
    .option("startingOffsets",         "latest") \
    .option("failOnDataLoss",          "false") \
    .load()

json_df = (
    kafka_df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("d"))
      .select("d.*")
      .withColumn("event_time", col("timestamp").cast(TimestampType()))
)



# ----------------------------------------------------------------------------------
# 2) analising some patterns on COMMISSION of the transactions
#    patterns:
#    A --  Total commission by type per minute.
#    B --v Commission ratio (commission/transaction amount) by merchant category
#    C --  Highest commission-generating merchants in 5-minute windows.
# ----------------------------------------------------------------------------------

# A. Total commission
comm_type_df = (
    json_df
      .withWatermark("event_time", "1 minute")
      .groupBy(window(col("event_time"), "1 minute"), col("commission_type"))
      .agg(_sum("commission_amount").alias("total_commission"))
)

# B. Commission‑to‑amount ratio
comm_ratio_df = (
    json_df
      .withWatermark("event_time", "1 minute")
      .groupBy(window(col("event_time"), "1 minute"), col("merchant_category"))
      .agg(
         (_sum("commission_amount") / _sum("amount"))
           .alias("commission_ratio")
      )
)

# C. Top commission merchants
top_merchants_df = (
    json_df
      .withWatermark("event_time", "5 minutes")
      .groupBy(window(col("event_time"), "5 minutes"), col("merchant_id"))
      .agg(_sum("commission_amount").alias("commission_sum"))
)


for df, checkpoint in [
    (comm_type_df,   "checkpoint/comm/type/kafka"),
    (comm_ratio_df,  "checkpoint/comm/ratio/kafka"),
    (top_merchants_df, "checkpoint/comm/top/kafka")
]:
    df.select(to_json(struct("*")).alias("value")) \
      .writeStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("topic", "darooghe.realTimeAnalytics") \
      .option("checkpointLocation", checkpoint) \
      .start()


timer_amount = 60
spark.streams.awaitAnyTermination(timer_amount)
print(f"Real Time Commission Analytics terminated after {timer_amount} seconds.")
spark.stop()