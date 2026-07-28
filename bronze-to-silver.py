# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# Bronze -> Silver: clean the good rows, quarantine the bad ones.
# apply_dq_checks + write_delta come from ./utils (loaded in the cell above).
# NOTE: %run must sit alone in its own cell with nothing above it.
from pyspark.sql.functions import col, trim

bronze_df = spark.table("payments_catalog.bronze.payments_raw")
good_df, bad_df = apply_dq_checks(bronze_df)     # split good vs bad (see ./utils)

# transform only the good data before saving it to the silver table
silver_df = (
    good_df.dropDuplicates()
    .withColumn("transaction_id", trim(col("transaction_id")))
    .withColumn("payment_amount", col("payment_amount").cast("double"))
)

# good rows -> silver table
write_delta(silver_df, "payments_catalog.silver.payments_cleaned", mode="append")

# bad rows -> exceptions table, so users can query them via SQL or wire up
# dashboard alerts / emails when new exceptions arrive.
write_delta(bad_df, "payments_catalog.silver.payments_exceptions", mode="append")

print("Moved good rows to silver, bad rows to exceptions")
