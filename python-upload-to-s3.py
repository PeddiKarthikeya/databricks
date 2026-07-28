# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# Generate mock payment records and upload them to S3 as a CSV.
# upload_to_s3 comes from ./utils (loaded above) -> keeps this notebook short.
import csv, io, random, uuid
from datetime import datetime

BUCKET = "BUCKET"           # <- set your S3 bucket name
KEY = "raw/payments.csv"    # <- destination path inside the bucket

headers = ["transaction_id", "user_id", "merchant_id", "timestamp",
           "payment_amount", "currency", "payment_method",
           "card_number", "transaction_status"]

buf = io.StringIO()
writer = csv.writer(buf)
writer.writerow(headers)

now = datetime.now()
for _ in range(random.randint(10, 20)):          # a random handful of rows
    writer.writerow([
        f"txn-{uuid.uuid4().hex[:6]}",
        f"U-{random.randint(100, 999)}",
        f"M-{random.randint(500, 505)}",
        now.strftime("%Y/%m/%d %I:%M %p"),
        round(random.uniform(-50.0, 5000.0), 2),
        random.choice(["USD", "EUR", "INR", "GBP"]),
        random.choice(["Credit Card", "PayPal", "UPI"]),
        "".join([str(random.randint(0, 9)) for _ in range(16)]),
        random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[70, 15, 15])[0],
    ])

upload_to_s3(BUCKET, KEY, buf.getvalue())        # push the CSV to S3
