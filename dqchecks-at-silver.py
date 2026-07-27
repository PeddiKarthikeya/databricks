# Databricks notebook source
from pyspark.sql.functions import col, lit, when, length
def apply_dq_checks(df):
    invalid_txn = ~col("transaction_id").startswith("txn-")
    invalid_amount = col("payment_amount") <= 0
    invalid_currency = ~col("currency").isin(["USD", "EUR", "INR", "GBP"])
    invalid_status = ~col("transaction_status").isin(["SUCCESS", "FAILED", "PENDING"])
    invalid_card = length(col("card_number").cast("string")) != 16

    checked_df = df.withColumn(
        "dq_error_reason",
        when(invalid_txn, lit("Invalid Transaction ID prefix"))
        .when(invalid_amount, lit("Amount is zero or negative"))
        .when(invalid_currency, lit("Unsupported Currency"))
        .when(invalid_status, lit("Unknown Transaction Status"))
        .when(invalid_card, lit("Invalid Card Number Length"))
        .otherwise(lit("PASS"))
    )

    good_records = checked_df.filter(col("dq_error_reason") == "PASS").drop("dq_error_reason")
    bad_records = checked_df.filter(col("dq_error_reason") != "PASS")

    return good_records, bad_records


#IMPORTANT_POINT: this checks are applied across entire data frame records at once or across all records at once, not like checking row by row like using for loop, this are applied to all rows or entire data at once using vectorized processing, meaning, it apply this rules to all rows across your cluster memory using parallelization
