# Databricks notebook source
import pandas as pd
import boto3
import io

s3 = boto3.client(
    's3',
    aws_access_key_id='ACCESSKEY',
    aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
    region_name='us-east-2'
)

response = s3.get_object(
    Bucket='BUCKET_NAME',
    Key='KEY_NAME'
)

pandas_df = pd.read_csv(io.BytesIO(response['Body'].read()))
raw_df = spark.createDataFrame(pandas_df)

#display(raw_df)
raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("payments_catalog.bronze.payments_raw")

print("Data successfully saved as delta atble")



#IMPORTANT POINTS FOR UNDERSTNDING:
#Note: first we need to fetch the data from s3 into dataframe(our OS runs this application and store this dataframe using our pyhsical memory allocated for this application), and then, convert this datafrae into delta table(so that it can store permanently inside cloud)
#we use boto3 to establish a connection with aws using access key and secret key, and we are fetching the data from s3 bucket using get_object method, and once you read data, you convert to delta table(to store permanently into cloud)->you know that before storing data into cloud directly,we need to convert it in a temporary data frame, and you need to convert this to delta table using format("delta")
