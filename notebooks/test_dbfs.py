# Databricks notebook source
df=spark.read.csv("s3://adroit-training-analytics1-trainingdatabucket-1g88r82t5o2h9/training_data/u__sli.notif_email_unsub_data__2022_06_07__2022_06_15/*")
display(df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks/mlflow-tracking/1146126217070430/d20b661b42f0477d9195ec2a89eafb0f/artifacts/feature_stats.tsv

# COMMAND ----------

spark.read.csv("dbfs:/databricks/mlflow-tracking/1146126217070430/d20b661b42f0477d9195ec2a89eafb0f/artifacts/feature_stats.tsv").count()

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/flights/

# COMMAND ----------

spark.read.csv("dbfs:/databricks-datasets/flights/departuredelays.csv").count()

# COMMAND ----------

import pandas as pd
pd.read_csv('/dbfs/databricks-datasets/flights/departuredelays.csv', sep=',')

# COMMAND ----------

pd.read_csv('/dbfs/databricks/mlflow-tracking/1146126217070430/d20b661b42f0477d9195ec2a89eafb0f/artifacts/feature_stats.tsv', sep='\t')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE u__saurabh.test_table_2(
# MAGIC   `id` string, 
# MAGIC   `num` int)
# MAGIC   ROW FORMAT SERDE 
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
# MAGIC STORED AS INPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
# MAGIC OUTPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema u_saurabh_dbr;

# COMMAND ----------

# MAGIC %sql show schema u__saurabh

# COMMAND ----------


