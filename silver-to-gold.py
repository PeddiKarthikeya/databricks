# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# Silver -> Gold: build the aggregated business summary tables.
# write_delta comes from ./utils (loaded above).
from pyspark.sql.functions import col, sum, count, round

success_df = (
    spark.table("payments_catalog.silver.payments_cleaned")
    .filter(col("transaction_status") == "SUCCESS")     # only successful payments
)

# revenue + successful-transaction count per merchant & currency
merchant_gold_df = success_df.groupBy("merchant_id", "currency").agg(
    round(sum("payment_amount"), 2).alias("total_revenue"),
    count("transaction_id").alias("successful_transactions"),
)

# usage count + amount processed per payment method
payment_method_gold_df = success_df.groupBy("payment_method").agg(
    count("transaction_id").alias("usage_count"),
    round(sum("payment_amount"), 2).alias("total_processed_amount"),
)

write_delta(merchant_gold_df, "payments_catalog.gold.merchant_revenue_summary")
write_delta(payment_method_gold_df, "payments_catalog.gold.payment_method_summary")
print("Gold tables created successfully!")
