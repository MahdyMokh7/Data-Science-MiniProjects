from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col, when

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("CommissionAnalysis") \
    .getOrCreate()

# Step 2: Load historical transactions from JSON
df = spark.read.json("data/transactions.json")

# Step 3: Calculate total commission per merchant category (real data)
total_commission = df.groupBy("merchant_category") \
    .agg(sum("commission_amount").alias("total_commission"))

# Step 4: Calculate average commission per transaction per category
avg_commission = df.groupBy("merchant_category") \
    .agg(avg("commission_amount").alias("avg_commission"))

# Step 5: Calculate average commission-to-transaction ratio
agg_df = (
    df
    .groupBy("merchant_category")
    .agg(
        sum("commission_amount").alias("total_commission"),
        sum("amount").alias("total_amount")        # use `amount` not `total_amount`
    )
)
avg_ratio = (
    agg_df
    .withColumn(
        "commission_to_transaction_ratio",
        col("total_commission") / col("total_amount")
    )
)
avg_ratio = avg_ratio.select(
    "merchant_category",
    "commission_to_transaction_ratio"
)
avg_ratio.show(truncate=False)


# Step 6: Join all results using merchant_category as the key
report = total_commission \
    .join(avg_commission, on="merchant_category") \
    .join(avg_ratio, on="merchant_category")

# Step 7: Show real commission report
print("=== Actual Commission Report ===")
report.show(truncate=False)
report.write.mode("overwrite").json("output/CommissionAnalysis/real_commission")

# Step 8: Simulate a new tiered commission model
#  - < 500,000 → 2%
#  - 500,000–1,000,000 → 3%
#  - > 1,000,000 → 5%

df_simulated = df.withColumn(
    "simulated_commission",
    when(df.amount < 500_000, df.amount * 0.02)
    .when((df.amount >= 500_000) & (df.amount < 1_000_000), df.amount * 0.03)
    .otherwise(df.amount * 0.05)
)

# Step 9: Compare real vs simulated per merchant category
comparison = df_simulated.groupBy("merchant_category").agg(
    sum("commission_amount").alias("real_total_commission"),
    sum("simulated_commission").alias("simulated_total_commission"),
    avg("commission_amount").alias("real_avg_commission"),
    avg("simulated_commission").alias("simulated_avg_commission")
)

# Add difference column
comparison = comparison.withColumn(
    "delta_commission",
    col("simulated_total_commission") - col("real_total_commission")
)

# Step 10: Show comparison
print("=== Simulated vs Real Commission ===")
comparison.show(truncate=False)

# Step 11: Optionally save results
comparison.write.mode("overwrite").json("output/CommissionAnalysis/simulated_vs_real_commission")
