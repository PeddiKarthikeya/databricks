# Databricks notebook source
from pyspark.sql.functions import col, sum, count, round

silver_df = spark.table("payments_catalog.silver.payments_cleaned")
success_df = silver_df.filter(col("transaction_status") == "SUCCESS")

merchant_gold_df = (
    success_df.groupBy("merchant_id", "currency")
    .agg(
        round(sum("payment_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("successful_transactions")
    )
)

payment_method_gold_df = (
    success_df.groupBy("payment_method")
    .agg(
        count("transaction_id").alias("usage_count"),
        round(sum("payment_amount"), 2).alias("total_processed_amount")
    )
)

merchant_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("payments_catalog.gold.merchant_revenue_summary")

payment_method_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("payments_catalog.gold.payment_method_summary")

print("Gold tables created successfully!")
