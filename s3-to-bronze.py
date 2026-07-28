# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# S3 -> Bronze: read the raw CSV from S3 and land it as a Delta table.
# read_s3_csv + write_delta live in ./utils (loaded above), so this stays tiny.
#
# IMPORTANT (why two steps): we first pull the S3 data into a DataFrame
# (held in cluster memory), then persist it as a Delta table so it lives
# permanently in the cloud / catalog.

BUCKET = "BUCKET_NAME"   # <- set your S3 bucket name
KEY = "KEY_NAME"         # <- path of the raw CSV inside the bucket

raw_df = read_s3_csv(BUCKET, KEY)          # S3 CSV -> Spark DataFrame
# display(raw_df)
write_delta(raw_df, "payments_catalog.bronze.payments_raw", mode="overwrite")
