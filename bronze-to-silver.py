# Databricks notebook source
from pyspark.sql.functions import col, trim

# COMMAND ----------

# MAGIC %run ./dqchecks-at-silver

# COMMAND ----------

#the above cell is like declaration, it will fetch the dq function from another notebook "silver_layer_dq_checks" and loads in current OS memory so that it can execute within current notebook
#simply, it will fetch the dq_checks notebook currently into our memory, you can use it , suppose it have 10 diff functions, it will brong all of them
#NOTE: when you use %run make sure the above line of %run is not contain single space, single space line or comment , nothing should be left above %run

# COMMAND ----------

bronze_df = spark.table("payments_catalog.bronze.payments_raw")
good_df, bad_df = apply_dq_checks(bronze_df)

silver_df = (  #we do apply transformations on good data, and we move bad data into exception table, so that user can apply sql quesries to view list of all exceptions
    good_df.dropDuplicates()
    .withColumn("transaction_id", trim(col("transaction_id")))
    .withColumn("payment_amount", col("payment_amount").cast("double"))
)
#add ts later

#we save good data into silver table
silver_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("payments_catalog.silver.payments_cleaned")

# we save bad data into excepions_table, or silver.exceptions_table, as this exceptions are rasied in silver layer
#when we store the exception records in silver layer, user can view it any time using sql commands like sleect * from exception_table
#user can also connect to dashboard and he can receive alerts on this data, when exception come, he can get email when he subscribe to that dashboard
bad_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("payments_catalog.silver.payments_exceptions")

print("Finally, moved Good rows to silver table and bad rows to exception_table")
