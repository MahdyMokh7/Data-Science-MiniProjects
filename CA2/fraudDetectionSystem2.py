import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, expr, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

# ------------------------------------------------------------
# 1) Build spark session + Read from kafka + convert to JSON
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("RealTimeProcessing_FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("location", StructType([
        StructField("lat", DoubleType()),
        StructField("lng", DoubleType())
    ])),
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
# 2) Detecting fruad transactions 
#    logic:
#    A -- velocity check: if greater than 5 transactions per customer in 2min
#    B -- Geographical impossibility: : Transactions from locations >50 km apart within 5m
#    C -- Amount anomaly: Transaction amount > 10 time of customer's average
# ----------------------------------------------------------------------------------

# A-- velocity check
THRESHOLD = 2
vel_df = (
    json_df
      .withWatermark("event_time", "2 minutes")
      .groupBy(window(col("event_time"), "2 minutes"), col("customer_id"))
      .count()
      .filter(col("count") > THRESHOLD)
)

# B-- Geo check
geo_df = (
    json_df.alias("a")
      .join(json_df.alias("b"), expr("""
        a.customer_id = b.customer_id
        AND a.event_time BETWEEN b.event_time AND b.event_time + INTERVAL 5 minutes
        AND (6371*2*ASIN(
          SQRT(
            POWER(SIN(RADIANS(a.location.lat-b.location.lat)/2),2)
            + COS(RADIANS(b.location.lat))*COS(RADIANS(a.location.lat))*
              POWER(SIN(RADIANS(a.location.lng-b.location.lng)/2),2)
           )
        )) > 50
      """))
      .select(
         col("a.transaction_id").alias("tx1"),
         col("b.transaction_id").alias("tx2"),
         col("a.customer_id"),
         col("a.event_time").alias("time1"),
         col("b.event_time").alias("time2")
      )
)

# C-- Amount anomaly
avg_df = spark.createDataFrame([("cust1",100.0)],["customer_id","avg_amount"])
amt_df = (
    json_df.join(avg_df, "customer_id")
           .filter(col("amount") > col("avg_amount")*10)
           .select(
             col("transaction_id").alias("tx1"),
             col("customer_id"),
             col("amount").alias("tx_amt"),
             col("avg_amount").alias("cust_avg")
           )
)

fraud_df = vel_df.unionByName(geo_df, allowMissingColumns=True) \
                 .unionByName(amt_df, allowMissingColumns=True)


fraud_df.select(to_json(struct("*")).alias("value")) \
       .writeStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "localhost:9092") \
       .option("topic", "darooghe.fraud_alerts") \
       .option("checkpointLocation", "checkpoint/fraud/kafka") \
       .start()


timer_amount = 60
spark.streams.awaitAnyTermination(timer_amount)
print(f"Fraud detection terminated after {timer_amount} seconds.")
spark.stop()