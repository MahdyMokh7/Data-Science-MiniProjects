import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, to_json, struct,
    sum as _sum, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

# -----------------------------------------------------------------------------
# 1. Spark session with Kafka package
# -----------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RealTimeProcessingLayer") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------------------------
# 2. Read from transactions topic
# -----------------------------------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "darooghe.transactions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Schema for the JSON payload
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

# Parse JSON + cast timestamp
json_df = (
    kafka_df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .withColumn("event_time", col("timestamp").cast(TimestampType()))
)

# -----------------------------------------------------------------------------
# STEP 1: WINDOWED INSIGHTS (counts per merchant_category)
# -----------------------------------------------------------------------------
windowed_df = (
    json_df
      .withWatermark("event_time", "1 minute")
      .groupBy(
          window(col("event_time"), "1 minute", "20 seconds"),
          col("merchant_category")
      )
      .count()
)

# — console sink (for dev)
q1_console = (
    windowed_df.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", False)
      .option("checkpointLocation", "checkpoint/insights/console")
      .start()
)

# — kafka sink
q1_kafka = (
    windowed_df
      .select(to_json(struct("window","merchant_category","count")).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "darooghe.streamApp")
      .option("checkpointLocation", "checkpoint/insights/kafka")
      .start()
)

# -----------------------------------------------------------------------------
# STEP 2: FRAUD DETECTION
# -----------------------------------------------------------------------------
# A. Velocity: >5 txns per customer in 2 minutes
vel_df = (
    json_df
      .withWatermark("event_time", "2 minutes")
      .groupBy(window(col("event_time"), "2 minutes"), col("customer_id"))
      .count()
      .filter(col("count") > 5)
)

# B. Geo‑impossible: >50 km apart within 5 minutes
#    (simple self‑join with a 5 min window; compute Haversine in SQL expr)
geo_df = (
    json_df.alias("a")
      .join(
        json_df.alias("b"),
        expr("""
          a.customer_id = b.customer_id AND
          a.event_time BETWEEN b.event_time AND b.event_time + INTERVAL 5 minutes AND
          ( 6371 * 2 * ASIN(
              SQRT(
                POWER(SIN(RADIANS(a.location.lat - b.location.lat)/2),2) +
                COS(RADIANS(b.location.lat)) *
                COS(RADIANS(a.location.lat)) *
                POWER(SIN(RADIANS(a.location.lng - b.location.lng)/2),2)
              )
            )
          ) > 50
        """)
      )
      .select(
         col("a.transaction_id").alias("tx1"),
         col("b.transaction_id").alias("tx2"),
         col("a.customer_id"),
         col("a.event_time").alias("time1"),
         col("b.event_time").alias("time2")
      )
)

# C. Amount anomaly: >1000% of customer's historical average
#    (assume avg_df loaded externally; here we mock a static average lookup)
#    For demo, hard‑code a DataFrame avg_df(customer_id, avg_amount)
avg_df = spark.createDataFrame(
    [("cust1", 100.0), ("cust2", 50.0)], ["customer_id","avg_amount"]
)
amt_df = (
    json_df.join(avg_df, "customer_id")
           .filter(col("amount") > col("avg_amount")*10)
           .select("transaction_id","customer_id","amount","avg_amount")
)

fraud_df = vel_df.unionByName(
               geo_df.select("customer_id","tx1","tx2","time1","time2")
             , allowMissingColumns=True
           ).unionByName(
               amt_df.withColumnRenamed("transaction_id","tx1")
                     .withColumnRenamed("amount","tx_amt")
                     .withColumnRenamed("avg_amount","cust_avg"),
                     allowMissingColumns=True
           )

q2_fraud = (
    fraud_df
      .select(to_json(struct("*")).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "darooghe.fraud_alerts")
      .option("checkpointLocation", "checkpoint/fraud/kafka")
      .start()
)

# -----------------------------------------------------------------------------
# STEP 3: COMMISSION ANALYTICS
# -----------------------------------------------------------------------------
# A. Total commission by type per minute
comm_type_df = (
    json_df
      .withWatermark("event_time", "1 minute")
      .groupBy(window(col("event_time"), "1 minute"), col("commission_type"))
      .agg(_sum("commission_amount").alias("total_commission"))
)

# B. Commission ratio by merchant_category per minute
comm_ratio_df = (
    json_df
      .withWatermark("event_time", "1 minute")
      .groupBy(window(col("event_time"), "1 minute"), col("merchant_category"))
      .agg(
         (_sum("commission_amount")/_sum("amount")).alias("commission_ratio")
      )
)

# C. Top commission merchants in 5‑min windows
top_merchants_df = (
    json_df
      .withWatermark("event_time", "5 minutes")
      .groupBy(window(col("event_time"), "5 minutes"), col("merchant_id"))
      .agg(_sum("commission_amount").alias("commission_sum"))
      .orderBy(col("commission_sum").desc())
)

# — console sinks
q3_console_a = comm_type_df.writeStream \
                  .outputMode("update") \
                  .format("console") \
                  .option("truncate", False) \
                  .option("checkpointLocation", "checkpoint/comm/type/console") \
                  .start()

q3_console_b = comm_ratio_df.writeStream \
                  .outputMode("update") \
                  .format("console") \
                  .option("truncate", False) \
                  .option("checkpointLocation", "checkpoint/comm/ratio/console") \
                  .start()

q3_console_c = top_merchants_df.writeStream \
                  .outputMode("complete") \
                  .format("console") \
                  .option("truncate", False) \
                  .option("checkpointLocation", "checkpoint/comm/top/console") \
                  .start()

# — kafka sinks
q3_kafka_a = comm_type_df.select(to_json(struct("*")).alias("value")) \
                  .writeStream.format("kafka") \
                  .option("kafka.bootstrap.servers","localhost:9092") \
                  .option("topic","darooghe.realTimeAnalytics") \
                  .option("checkpointLocation","checkpoint/comm/type/kafka") \
                  .start()

q3_kafka_b = comm_ratio_df.select(to_json(struct("*")).alias("value")) \
                  .writeStream.format("kafka") \
                  .option("kafka.bootstrap.servers","localhost:9092") \
                  .option("topic","darooghe.realTimeAnalytics") \
                  .option("checkpointLocation","checkpoint/comm/ratio/kafka") \
                  .start()

q3_kafka_c = top_merchants_df.select(to_json(struct("*")).alias("value")) \
                  .writeStream.format("kafka") \
                  .option("kafka.bootstrap.servers","localhost:9092") \
                  .option("topic","darooghe.realTimeAnalytics") \
                  .option("checkpointLocation","checkpoint/comm/top/kafka") \
                  .start()

spark.streams.awaitAnyTermination(120)  # The application will run for 120 seconds and exit gracefully
print("Streaming application terminated after 120 seconds.")

# Ensure Spark stops gracefully
spark.stop()