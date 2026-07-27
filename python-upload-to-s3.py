# Databricks notebook source
import boto3
import csv
import io
import random
import uuid
from datetime import datetime
def generate_and_upload_mock_data():
    # Keys are pulled from the Databricks secret scope "aws" — never hardcode them.
    aws_access_key = dbutils.secrets.get(scope="aws", key="access_key")
    aws_secret_key = dbutils.secrets.get(scope="aws", key="secret_key")
    bucket_name = "BUCKET"
    region = "us-east-2"
    current_time = datetime.now()
    file_name = "raw/payments.csv"
    headers = ["transaction_id", "user_id", "merchant_id", "timestamp", "payment_amount", "currency", "payment_method", "card_number", "transaction_status"]
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    num_rows = random.randint(10, 20)
    for _ in range(num_rows):
        writer.writerow([
            f"txn-{uuid.uuid4().hex[:6]}",
            f"U-{random.randint(100, 999)}",
            f"M-{random.randint(500, 505)}",
            current_time.strftime("%Y/%m/%d %I:%M %p"),
            round(random.uniform(-50.0, 5000.0), 2),
            random.choice(["USD", "EUR", "INR", "GBP"]),
            random.choice(["Credit Card", "PayPal", "UPI"]),
            "".join([str(random.randint(0, 9)) for _ in range(16)]),
            random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[70, 15, 15])[0]
        ])
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=csv_buffer.getvalue()
    )
    print(f"Successfully uploaded {num_rows} records to s3://{bucket_name}/{file_name}")

generate_and_upload_mock_data()
