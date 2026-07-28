# Databricks notebook source
# =====================================================================
# utils  ->  shared helper functions for the payments pipeline
# ---------------------------------------------------------------------
# All the repeated "how" lives here so every pipeline notebook stays
# short and only expresses WHAT it does. Load it in any notebook with:
#     %run ./utils
# =====================================================================
import boto3
import io
import pandas as pd
from pyspark.sql.functions import col, lit, when, length


# --- AWS / S3 helpers -------------------------------------------------
def get_s3_client(region="us-east-2"):
    # boto3 S3 client built from keys in the Databricks secret scope "aws"
    # so credentials never sit inside notebook code.
    return boto3.client(
        "s3",
        aws_access_key_id=dbutils.secrets.get(scope="aws", key="access_key"),
        aws_secret_access_key=dbutils.secrets.get(scope="aws", key="secret_key"),
        region_name=region,
    )


def upload_to_s3(bucket, key, body, region="us-east-2"):
    # Upload an in-memory body (str/bytes) to s3://bucket/key.
    get_s3_client(region).put_object(Bucket=bucket, Key=key, Body=body)
    print(f"Uploaded -> s3://{bucket}/{key}")


def read_s3_csv(bucket, key, region="us-east-2"):
    # Read a CSV from S3 into a Spark DataFrame (pandas is the bridge:
    # first into memory as a pandas df, then converted to a Spark df).
    obj = get_s3_client(region).get_object(Bucket=bucket, Key=key)
    pdf = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return spark.createDataFrame(pdf)


# --- Delta helper -----------------------------------------------------
def write_delta(df, table, mode="overwrite"):
    # Persist a DataFrame as a Delta table (permanent storage in the catalog).
    df.write.format("delta").mode(mode).saveAsTable(table)
    print(f"{mode} -> {table}")


# --- Data-quality helper ----------------------------------------------
def apply_dq_checks(df):
    # Tag every row PASS / fail-reason, then split into good vs bad records.
    # IMPORTANT: these checks run on the WHOLE DataFrame at once (vectorized,
    # parallelized across the cluster) -- not row-by-row in a Python loop.
    invalid_txn = ~col("transaction_id").startswith("txn-")
    invalid_amount = col("payment_amount") <= 0
    invalid_currency = ~col("currency").isin(["USD", "EUR", "INR", "GBP"])
    invalid_status = ~col("transaction_status").isin(["SUCCESS", "FAILED", "PENDING"])
    invalid_card = length(col("card_number").cast("string")) != 16

    checked = df.withColumn(
        "dq_error_reason",
        when(invalid_txn, lit("Invalid Transaction ID prefix"))
        .when(invalid_amount, lit("Amount is zero or negative"))
        .when(invalid_currency, lit("Unsupported Currency"))
        .when(invalid_status, lit("Unknown Transaction Status"))
        .when(invalid_card, lit("Invalid Card Number Length"))
        .otherwise(lit("PASS")),
    )
    good = checked.filter(col("dq_error_reason") == "PASS").drop("dq_error_reason")
    bad = checked.filter(col("dq_error_reason") != "PASS")
    return good, bad
