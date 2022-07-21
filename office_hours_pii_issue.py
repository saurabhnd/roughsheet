# Databricks notebook source
df = spark.read.format("text").load("s3://prd-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__1hours/tracking_v2_ad_response_pii/20220606T180000/")
df.count()

# COMMAND ----------

df = spark.read.format("text").load("s3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__1hours/tracking_v2_ad_response_pii/20220606T180000/")
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------


