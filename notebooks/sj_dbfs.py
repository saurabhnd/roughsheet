# Databricks notebook source
# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %fs ls cluster-logs/0406-231321-9o4ygyy1/init_scripts/0406-231321-9o4ygyy1_100_101_3_171/

# COMMAND ----------

# MAGIC %fs cp dbfs:/cluster-logs/0406-231321-9o4ygyy1/init_scripts/0406-231321-9o4ygyy1_100_101_3_171/20220406_231354_00_init.sh.stderr.log file:/Users/sjain/work/logs/init_scripts/init_script.stderr.log

# COMMAND ----------

# MAGIC %fs ls dbfs:/cluster-logs/0406-231321-9o4ygyy1/init_scripts/0406-231321-9o4ygyy1_100_101_3_171/20220406_231354_00_init.sh.stderr.log

# COMMAND ----------

# MAGIC %fs ls databricks/scripts

# COMMAND ----------

print(spark.conf.get("spark.hadoop.javax.jdo.option.ConnectionURL"))
print(spark.conf.get("spark.hadoop.javax.jdo.option.ConnectionUserName"))
print(spark.conf.get("spark.hadoop.javax.jdo.option.ConnectionDriverName"))

# COMMAND ----------

# MAGIC %fs ls cluster-logs/

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/

# COMMAND ----------


