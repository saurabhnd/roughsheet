# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------


target =spark.read.orc("s3://prd-pubsub-raw-orc-datalake-analytics-nextdoor-com/account__samwise__account/v=002/arrivaltime=2022051418/region=prd") 

target = target.withColumnRenamed("_col0", "id")

target = target.withColumnRenamed("_col1", "email")

target = target.withColumnRenamed("_col2", "last_modified")

target = target.withColumnRenamed("_col3", "locale")

target = target.withColumnRenamed("_col4", "profiles")

target = target.withColumnRenamed("_col5", "profile_ids")

target = target.withColumnRenamed("_col6", "social_id")

target = target.withColumnRenamed("_col7", "update_count")

target = target.withColumnRenamed("_col8", "username")

target = target.withColumnRenamed("_col9", "action")

target = target.withColumnRenamed("_col10", "is_deleted")

target = target.withColumnRenamed("_col11", "phone_number")

#target.show(10,False)
#4cdcb1fa-0660-4918-9a2e-e30df7a40b30
#target.count()

target.write.mode("overwrite").format("orc").save("s3://dev-pubsub-raw-orc-datalake-analytics-nextdoor-com/account__samwise__account/v=002/arrivaltime=2022051418/region=dev")

# COMMAND ----------

src = spark.read.orc("s3://dev-pubsub-raw-orc-datalake-analytics-nextdoor-com/account__samwise__account/v=002/arrivaltime=2022051418/region=dev") 

src.printSchema()

src_cached = src.cache()
src_cached.createOrReplaceTempView("src_data")

# COMMAND ----------

src_cached.count()

# COMMAND ----------

src_df= spark.sql("""select * from src_data""")
src_df.explain()
#src_df.filter(F.col('id') == '4cdcb1fa-0660-4918-9a2e-e30df7a40b30').count()

# COMMAND ----------

target =spark.read.orc("s3://prd-pubsub-raw-orc-datalake-analytics-nextdoor-com/account__samwise__account/v=002/arrivaltime=2022051418/region=prd") 

target = target.withColumnRenamed("_col0", "id")

target = target.withColumnRenamed("_col1", "email")

target = target.withColumnRenamed("_col2", "last_modified")

target = target.withColumnRenamed("_col3", "locale")

target = target.withColumnRenamed("_col4", "profiles")

target = target.withColumnRenamed("_col5", "profile_ids")

target = target.withColumnRenamed("_col6", "social_id")

target = target.withColumnRenamed("_col7", "update_count")

target = target.withColumnRenamed("_col8", "username")

target = target.withColumnRenamed("_col9", "action")

target = target.withColumnRenamed("_col10", "is_deleted")

target = target.withColumnRenamed("_col11", "phone_number")

#target.show(10,False)
#4cdcb1fa-0660-4918-9a2e-e30df7a40b30
#target.count()

target.write.mode("append").format("orc").save("s3://dev-pubsub-raw-orc-datalake-analytics-nextdoor-com/account__samwise__account/v=002/arrivaltime=2022051418/region=dev")

# COMMAND ----------

src_df_2= spark.sql("""select * from src_data""")

src_df_2.filter(F.col('id') == '4cdcb1fa-0660-4918-9a2e-e30df7a40b30').count()

# COMMAND ----------

src_df_2.count()

# COMMAND ----------


