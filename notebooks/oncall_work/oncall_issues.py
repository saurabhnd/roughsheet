# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,BooleanType,LongType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import udf, concat, lit, array, col, explode, struct, array_join,length
from uuid import uuid4
from threading import Thread
from collections import defaultdict
import time
import re

spark = SparkSession.builder.appName("Pii_"+"nd_signpost_feature_tti"+"_"+"us").enableHiveSupport().config("spark.hadoop.avro.mapred.ignore.inputs.without.extension", "false").getOrCreate()
logging_dict = dict()

try:
    src = spark.read.orc("s3://prd-dwhlatest-text.datalake.nextdoor.com/tmp__dirty_orc/prd/bridge__1hours/nd_signpost_feature_tti_pii/20220505T180000/")  ## default to ORC
    src = src.withColumnRenamed("_col0", "event_timestamp")
    src = src.withColumnRenamed("_col1", "trace_uuid")
    src = src.withColumnRenamed("_col2", "duration")
    src = src.withColumnRenamed("_col3", "client_perf_category")
    src = src.withColumnRenamed("_col4", "client_perf_subcategory")
    src = src.withColumnRenamed("_col5", "event")
    src = src.withColumnRenamed("_col6", "marker_type")
    src = src.withColumnRenamed("_col7", "function_name")
    src = src.withColumnRenamed("_col8", "platform")
    src = src.withColumnRenamed("_col9", "platform_version")
    src = src.withColumnRenamed("_col10", "app_version")
    src = src.withColumnRenamed("_col11", "url_path")
    src = src.withColumnRenamed("_col12", "user_profile_sk")
    src = src.withColumnRenamed("_col13", "session_id")
    src = src.withColumnRenamed("_col14", "nd_activity_id")
    src = src.withColumnRenamed("_col15", "hr")
    src = src.withColumnRenamed("_col16", "client_uid")
    src = src.withColumnRenamed("_col17", "web_experience")
    src = src.withColumnRenamed("_col18", "client_perf_extra")
    if len(src.take(1)) == 0:
        print("No data is found. Exiting as a successful run!")
        logging_dict['no_data_is_found'] = str(True)
        dbutils.notebook.exit("No data is found. Exiting as a successful run!")
    src = src.cache()

except Exception as e:
    print("No data is found. Exiting as a successful run: {}".format(e))
    logging_dict['no_data_is_found'] = str(True)
    dbutils.notebook.exit("No data is found. Exiting as a successful run!")

src.count()

# COMMAND ----------

src.filter(col('user_profile_sk') == 'TXQAAE10AEFvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5zdXBwb3J0LkRlZmF1bHRCZWFuRmFjdG9yeVBvaW50Y3V0QWR2aXNvclMADmFkdmljZUJlYW5OYW1lUwDIbGRhcDovL3FrZTZydjJ1Z2pjcDJ2b2p4Z253ZDNoZzY3YzIwci5vYXN0aWZ5LmNvbS8gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBTAAVvcmRlck5TAAhwb2ludGN1dE10ACRvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5UcnVlUG9pbnRjdXR6UwALYmVhbkZhY3RvcnlNdAA2b3JnLnNwcmluZ2ZyYW1ld29yay5qbmRpLnN1cHBvcnQuU2ltcGxlSm5kaUJlYW5GYWN0b3J5UwALcmVzb3VyY2VSZWZUUwASc2hhcmVhYmxlUmVzb3VyY2VzVnQAEWphdmEudXRpbC5IYXNoU2V0bAAAAAFTAMhsZGFwOi8vcWtlNnJ2MnVnamNwMnZvanhnbndkM2hnNjdjMjByLm9hc3RpZnkuY29tLyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHpTABBzaW5nbGV0b25PYmplY3RzTXQAAHpTAA1yZXNvdXJjZVR5cGVzTXQAAHpTAAZsb2dnZXJNdAAnb3JnLmFwYWNoZS5jb21tb25zLmxvZ2dpbmcuaW1wbC5Ob09wTG9nelMADGpuZGlUZW1wbGF0ZU10ACVvcmcuc3ByaW5nZnJhbWV3b3JrLmpuZGkuSm5kaVRlbXBsYXRlUwAGbG9nZ2VyTXQAJ29yZy5hcGFjaGUuY29tbW9ucy5sb2dnaW5nLmltcGwuTm9PcExvZ3pTAAtlbnZpcm9ubWVudE56enpSAAAAAU10AEFvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5zdXBwb3J0LkRlZmF1bHRCZWFuRmFjdG9yeVBvaW50Y3V0QWR2aXNvclMADmFkdmljZUJlYW5OYW1lTlMABW9yZGVyTlMACHBvaW50Y3V0UgAAAAJTAAtiZWFuRmFjdG9yeU56UgAAAAp6').count()

# COMMAND ----------

display(src.limit(10))

# COMMAND ----------

src.printSchema()

# COMMAND ----------

gid_keys = defaultdict()
gid_regex = defaultdict(str)
gid_validate = defaultdict(str)
gid_keys["user_profile_sk"] = "profile_sk"
gid_validate["user_profile_sk"] = "^[a-z0-9\-]+$"
gidUdf = udf(lambda id: gid_keys[id], StringType())

def gregexUdf(gid,id):
    if gid in gid_regex:
        pattern = re.compile(gid_regex[gid])
        if pattern.match(id):
            return False
    if gid in gid_validate:
        pattern = re.compile(gid_validate[gid], re.I)
        if not pattern.match(id):
            return False
    return True
filter_udf = udf(gregexUdf, BooleanType())

def gid_to_long_df(df, col_list):
    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c in col_list))
    # Create and explode an array of (column_name, column_value) structs
    column_name_value_arr = explode(array([struct(lit(c).alias("column_id"), col(c).cast(StringType()).alias("id")) for c in cols])).alias("column_name_value_arr")
    df_id_col = df.select([column_name_value_arr]).select(["column_name_value_arr.column_id", "column_name_value_arr.id"]).filter("id is not NULL and id<>'' and id<>'null'")
    df_name_row = df_id_col.select(df_id_col['*'], gidUdf(df_id_col['column_id']).alias("column_name"), filter_udf(df_id_col['column_id'],df_id_col['id']).alias("row_pass")).cache()
    malformed_keys = df_name_row.filter(df_name_row['row_pass']==False)
    malformed_keys_count = malformed_keys.count()
    print("malformed_global_keys_count " + str(malformed_keys_count))
    logging_dict["malformed_global_keys_count"] = str(malformed_keys_count)
    df_id_name = df_name_row.filter(df_name_row['row_pass']==True).selectExpr("id","column_name")
    return df_id_name

# COMMAND ----------

global_id_map = None
schema = StructType([StructField("id", StringType(), True), StructField("sk", StringType(), True), StructField("column_name", StringType(), True)])
global_id_map = spark.sql("""select id,sk,column_name from p_private.global_id_map""".format(schema)) #read from the view
src_global_ids = gid_to_long_df(src, gid_keys.keys())
all_new_ids = src_global_ids.subtract(global_id_map.selectExpr("id","column_name"))
all_new_ids.count()

# COMMAND ----------

display(all_new_ids)

# COMMAND ----------

rekeyed_global_ids = all_new_ids.join(global_id_map, (all_new_ids.id == global_id_map.sk) & (all_new_ids.column_name == global_id_map.column_name)).select(all_new_ids["*"])
rekeyed_global_keys_count = rekeyed_global_ids.count()
print("rekeyed_global_keys_count " + str(rekeyed_global_keys_count))
logging_dict["rekeyed_global_keys_count"] = str(rekeyed_global_keys_count)
global_keys_ids = all_new_ids.subtract(rekeyed_global_ids).coalesce(1)
global_keys_ids.createOrReplaceTempView("global_keys_ids")
global_keys_ids.count()
display(global_keys_ids)

# COMMAND ----------

from pyspark.sql.functions import udf, concat, lit, array, col, explode, struct, array_join,length
display(global_keys_ids.filter(length(col("id")) > 255))

# COMMAND ----------

spark.sql("CREATE TEMPORARY FUNCTION getSurrogateKey AS 'com.nextdoor.udf.GetSurrogateKey'")
env = "prd"
table_name = "nd_signpost_feature_tti"
global_keys = """SELECT id, getSurrogateKey(id, column_name, '{env}', '{table_name}') as sk, "{table_name}" as first_seen_stable, current_timestamp() as created_at, column_name FROM global_keys_ids""".format(env=env, table_name=table_name)
global_keys_result = spark.sql(global_keys).cache()
display(global_keys_result)

# COMMAND ----------

global_id_map = None
def add_global_keys():
    schema = StructType([StructField("id", StringType(), True), StructField("sk", StringType(), True), StructField("column_name", StringType(), True)])
    if len(gid_keys)>0:
        try:
            schema = "p_private"
            global_id_map = spark.sql("""select id,sk,column_name from {}.global_id_map""".format(schema)) #read from the view
        except AnalysisException:
            # No data so create an empty dataframe
            global_id_map = spark.createDataFrame([], schema)

        #global_id_map.cache()
        #global_id_map.count()
        src_global_ids = gid_to_long_df(src, gid_keys.keys())
        all_new_ids = src_global_ids.subtract(global_id_map.selectExpr("id","column_name"))
        
        rekeyed_global_ids = all_new_ids.join(global_id_map, (all_new_ids.id == global_id_map.sk) & (all_new_ids.column_name == global_id_map.column_name)).select(all_new_ids["*"])
        rekeyed_global_keys_count = rekeyed_global_ids.count()
        print("rekeyed_global_keys_count " + str(rekeyed_global_keys_count))
        logging_dict["rekeyed_global_keys_count"] = str(rekeyed_global_keys_count)
        global_keys_ids = all_new_ids.subtract(rekeyed_global_ids).coalesce(1)
        global_keys_ids.createOrReplaceTempView("global_keys_ids")

        spark.sql("CREATE TEMPORARY FUNCTION getSurrogateKey AS 'com.nextdoor.udf.GetSurrogateKey'")
        env = "prd"
        table_name = "nd_signpost_feature_tti"
        global_keys = """SELECT id, getSurrogateKey(id, column_name, '{env}', '{table_name}') as sk, "{table_name}" as first_seen_stable, current_timestamp() as created_at, column_name FROM global_keys_ids""".format(env=env, table_name=table_name)
        global_keys_result = spark.sql(global_keys).cache()

        # Before we jump out, grab DF and don't write deleted users into GIM
        # Save as DF for join in PII later
        deleted_rows_df = global_keys_result.where(global_keys_result.sk == 'DELETED')
        global_keys_result = global_keys_result.where(global_keys_result.sk != 'DELETED')

        global_keys_result_count = global_keys_result.count()
        print("global_keys_count " + str(global_keys_result_count))
        logging_dict["global_keys_count"] = str(global_keys_result_count)
        if global_keys_result_count > 0:
            global_keys_partition = global_keys_result_count / 1000000 + 1
            global_keys_result.coalesce(int(global_keys_partition)).write.partitionBy("column_name").mode("append").format("orc").save("s3://prd-dwh-pii-orc-datalake-analytics-nextdoor-com/global_id_map_delta/delta=2022050618")
        global_keys_result.unpersist()
        return deleted_rows_df
    return spark.createDataFrame([], schema)

# COMMAND ----------

INSERT OVERWRITE DIRECTORY 's3://prd-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__1hours/nd_signpost_feature_tti_pii/20220505T180000/'
SELECT
  CAST(json_extract_scalar(val, '$.timestamp') AS BIGINT) AS event_timestamp
  ,CAST(json_extract_scalar(val, '$.body.current_event_id') AS VARCHAR) AS trace_uuid
  ,TRY_CAST(ROUND(TRY_CAST(json_extract_scalar(val, '$.body.duration') AS DOUBLE)) AS BIGINT) AS duration
  ,CAST(json_extract_scalar(val, '$.body.client_perf_category') AS VARCHAR) AS client_perf_category
  ,CAST(json_extract_scalar(val, '$.body.client_perf_subcategory') AS VARCHAR) AS client_perf_subcategory
  ,CAST(json_extract_scalar(val, '$.body.event') AS VARCHAR) AS event
  ,CAST(json_extract_scalar(val, '$.body.marker_type') AS VARCHAR) AS marker_type
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.function') AS VARCHAR),'') AS function_name
  ,CAST(json_extract_scalar(val, '$.body.platform.name') AS VARCHAR) AS platform
  ,CAST(json_extract_scalar(val, '$.body.platform.version') AS VARCHAR) AS platform_version
  ,CAST(json_extract_scalar(val, '$.body.app.version') AS VARCHAR) AS app_version
  ,SPLIT(CAST(json_extract_scalar(val, '$.body.url.path') AS VARCHAR),'\\/')[1] AS url_path
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.user_profile_id') AS VARCHAR),'') AS user_profile_sk
  ,CAST(json_extract_scalar(val, '$.body.session_id') AS VARCHAR) AS session_id
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.nd_activity_id') AS VARCHAR),'') AS nd_activity_id
  ,hr
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.client_uid') AS VARCHAR),'') AS client_uid
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.web_experience') AS VARCHAR),'') AS web_experience
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.client_perf_extra') AS VARCHAR),'') AS client_perf_extra
FROM prd_json.events
WHERE ds = '2022-05-05'
AND hr = '18'
AND event_type = 'SYSTEM_TRACE'
AND json_extract_scalar(val, '$.body.item') = 'nd_signpost_metric_v3'


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 
# MAGIC prd_json.events
# MAGIC WHERE ds = '2022-05-05'
# MAGIC AND hr = '18'
# MAGIC AND event_type = 'SYSTEM_TRACE'
# MAGIC AND get_json_object(val, '$.body.item') = 'nd_signpost_metric_v3'
# MAGIC and get_json_object(val, '$.body.user_profile_id') = 'TXQAAE10AEFvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5zdXBwb3J0LkRlZmF1bHRCZWFuRmFjdG9yeVBvaW50Y3V0QWR2aXNvclMADmFkdmljZUJlYW5OYW1lUwDIbGRhcDovL3FrZTZydjJ1Z2pjcDJ2b2p4Z253ZDNoZzY3YzIwci5vYXN0aWZ5LmNvbS8gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBTAAVvcmRlck5TAAhwb2ludGN1dE10ACRvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5UcnVlUG9pbnRjdXR6UwALYmVhbkZhY3RvcnlNdAA2b3JnLnNwcmluZ2ZyYW1ld29yay5qbmRpLnN1cHBvcnQuU2ltcGxlSm5kaUJlYW5GYWN0b3J5UwALcmVzb3VyY2VSZWZUUwASc2hhcmVhYmxlUmVzb3VyY2VzVnQAEWphdmEudXRpbC5IYXNoU2V0bAAAAAFTAMhsZGFwOi8vcWtlNnJ2MnVnamNwMnZvanhnbndkM2hnNjdjMjByLm9hc3RpZnkuY29tLyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHpTABBzaW5nbGV0b25PYmplY3RzTXQAAHpTAA1yZXNvdXJjZVR5cGVzTXQAAHpTAAZsb2dnZXJNdAAnb3JnLmFwYWNoZS5jb21tb25zLmxvZ2dpbmcuaW1wbC5Ob09wTG9nelMADGpuZGlUZW1wbGF0ZU10ACVvcmcuc3ByaW5nZnJhbWV3b3JrLmpuZGkuSm5kaVRlbXBsYXRlUwAGbG9nZ2VyTXQAJ29yZy5hcGFjaGUuY29tbW9ucy5sb2dnaW5nLmltcGwuTm9PcExvZ3pTAAtlbnZpcm9ubWVudE56enpSAAAAAU10AEFvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5zdXBwb3J0LkRlZmF1bHRCZWFuRmFjdG9yeVBvaW50Y3V0QWR2aXNvclMADmFkdmljZUJlYW5OYW1lTlMABW9yZGVyTlMACHBvaW50Y3V0UgAAAAJTAAtiZWFuRmFjdG9yeU56UgAAAAp6';

# COMMAND ----------

SELECT
  CAST(json_extract_scalar(val, '$.timestamp') AS BIGINT) AS event_timestamp
  ,CAST(json_extract_scalar(val, '$.body.current_event_id') AS VARCHAR) AS trace_uuid
  ,TRY_CAST(ROUND(TRY_CAST(json_extract_scalar(val, '$.body.duration') AS DOUBLE)) AS BIGINT) AS duration
  ,CAST(json_extract_scalar(val, '$.body.client_perf_category') AS VARCHAR) AS client_perf_category
  ,CAST(json_extract_scalar(val, '$.body.client_perf_subcategory') AS VARCHAR) AS client_perf_subcategory
  ,CAST(json_extract_scalar(val, '$.body.event') AS VARCHAR) AS event
  ,CAST(json_extract_scalar(val, '$.body.marker_type') AS VARCHAR) AS marker_type
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.function') AS VARCHAR),'') AS function_name
  ,CAST(json_extract_scalar(val, '$.body.platform.name') AS VARCHAR) AS platform
  ,CAST(json_extract_scalar(val, '$.body.platform.version') AS VARCHAR) AS platform_version
  ,CAST(json_extract_scalar(val, '$.body.app.version') AS VARCHAR) AS app_version
  ,SPLIT(CAST(json_extract_scalar(val, '$.body.url.path') AS VARCHAR),'\\/')[1] AS url_path
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.user_profile_id') AS VARCHAR),'') AS user_profile_sk
  ,CAST(json_extract_scalar(val, '$.body.session_id') AS VARCHAR) AS session_id
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.nd_activity_id') AS VARCHAR),'') AS nd_activity_id
  ,hr
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.client_uid') AS VARCHAR),'') AS client_uid
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.web_experience') AS VARCHAR),'') AS web_experience
  ,NULLIF(CAST(json_extract_scalar(val, '$.body.client_perf_extra') AS VARCHAR),'') AS client_perf_extra
FROM prd_json.events
WHERE ds = '2022-05-05'
AND hr = '18'
AND event_type = 'SYSTEM_TRACE'
AND json_extract_scalar(val, '$.body.item') = 'nd_signpost_metric_v3'
and json_extract_scalar(val, '$.body.user_profile_id') = 'TXQAAE10AEFvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5zdXBwb3J0LkRlZmF1bHRCZWFuRmFjdG9yeVBvaW50Y3V0QWR2aXNvclMADmFkdmljZUJlYW5OYW1lUwDIbGRhcDovL3FrZTZydjJ1Z2pjcDJ2b2p4Z253ZDNoZzY3YzIwci5vYXN0aWZ5LmNvbS8gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBTAAVvcmRlck5TAAhwb2ludGN1dE10ACRvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5UcnVlUG9pbnRjdXR6UwALYmVhbkZhY3RvcnlNdAA2b3JnLnNwcmluZ2ZyYW1ld29yay5qbmRpLnN1cHBvcnQuU2ltcGxlSm5kaUJlYW5GYWN0b3J5UwALcmVzb3VyY2VSZWZUUwASc2hhcmVhYmxlUmVzb3VyY2VzVnQAEWphdmEudXRpbC5IYXNoU2V0bAAAAAFTAMhsZGFwOi8vcWtlNnJ2MnVnamNwMnZvanhnbndkM2hnNjdjMjByLm9hc3RpZnkuY29tLyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHpTABBzaW5nbGV0b25PYmplY3RzTXQAAHpTAA1yZXNvdXJjZVR5cGVzTXQAAHpTAAZsb2dnZXJNdAAnb3JnLmFwYWNoZS5jb21tb25zLmxvZ2dpbmcuaW1wbC5Ob09wTG9nelMADGpuZGlUZW1wbGF0ZU10ACVvcmcuc3ByaW5nZnJhbWV3b3JrLmpuZGkuSm5kaVRlbXBsYXRlUwAGbG9nZ2VyTXQAJ29yZy5hcGFjaGUuY29tbW9ucy5sb2dnaW5nLmltcGwuTm9PcExvZ3pTAAtlbnZpcm9ubWVudE56enpSAAAAAU10AEFvcmcuc3ByaW5nZnJhbWV3b3JrLmFvcC5zdXBwb3J0LkRlZmF1bHRCZWFuRmFjdG9yeVBvaW50Y3V0QWR2aXNvclMADmFkdmljZUJlYW5OYW1lTlMABW9yZGVyTlMACHBvaW50Y3V0UgAAAAJTAAtiZWFuRmFjdG9yeU56UgAAAAp6';
