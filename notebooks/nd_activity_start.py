# Databricks notebook source
from datetime import datetime
stmt = r"""CREATE TEMPORARY FUNCTION decrypt AS 'com.nextdoor.udf.NdCrypt'"""
print(f"""======================== Executing {datetime.now()} =======================""")
print(stmt)
print("""=====================================================================================\n""")
#spark.sql(stmt)
stmt = r"""SELECT DISTINCT
CAST(get_json_object(e.val, '$.timestamp') AS BIGINT) AS event_timestamp
,NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') AS user_profile_sk
,NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.cookies.WERC') AS STRING),'') AS session_cookie
,NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.cookies.WE') AS STRING),'') AS persistent_cookie
,ua.platform AS platform
,REGEXP_REPLACE(REGEXP_REPLACE(ua.user_agent, '\r', ''), '\n', ' ') AS user_agent -- no tabs or newlines
,ua.user_agent_category AS user_agent_category
,ua.device AS device
,ua.browser AS browser
,ua.browser_major AS browser_major
,ua.browser_minor AS browser_minor
,ua.os AS os
,ua.os_major AS os_major
,ua.os_minor AS os_minor
,ua.app_version AS app_version
,ua.app_version_major AS app_version_major
,ua.app_version_minor AS app_version_minor
,ua.app_version_patch AS app_version_patch
,DECRYPT(NULLIF(CAST(get_json_object(e.val, '$.body.activity_source.email_id') AS STRING),''), '[0-9]+') AS email_id
,NULLIF(CAST(get_json_object(e.val, '$.body.activity_source.source') AS STRING),'') AS nd_activity_source
,get_json_object(e.val, '$.body.activity_source') AS source_details
,NULLIF(CAST(get_json_object(e.val, '$.body.client_info.platform.name') AS STRING),'') AS platform_name
,NULLIF(CAST(get_json_object(e.val, '$.body.client_info.app.build_type') AS STRING),'') AS app_build_type
,NULLIF(CAST(get_json_object(e.val, '$.body.host') AS STRING),'') AS nd_site_host
,NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),'') AS client_uid
,NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),'') AS client_uid_hash
,NULLIF(UPPER(CAST(get_json_object(e.val, '$.body.nd_activity_id') AS STRING)),'') AS nd_activity_id
,'09' AS hr
,TRY_CAST(get_json_object(e.val, '$.body.activity_source.badge_count') AS INT) AS badge_count
FROM (
SELECT
  REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(e.val AS STRING),'/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003',' '),'\u0001') AS val -- remove all linebreaks, returns, tabs, etc.
FROM prd_json.events e
where ds = '2022-06-13' and e.event_type = 'ND_ACTIVITY_START' and val like '%4ff68f5f-0244-41aa-b797-655b5e88513822061%'
) e
LEFT JOIN prd_avro.user_agent ua
ON NULLIF(
    --REGEXP_REPLACE(REGEXP_REPLACE(
          COALESCE(
                CAST(get_json_object(e.val, '$.request.user_agent') AS STRING)
               ,CAST(get_json_object(e.val, '$.body.client_info.user_agent') AS STRING)
          )
    --      , '\r', ' '), '\n', ' ')
    ,'')
        = REGEXP_REPLACE(REGEXP_REPLACE(ua.user_agent, '\r', ' '), '\n', ' ')
AND ua.ds = '2022-06-16'
AND ua.hr = '09'
WHERE (  -- same as client_view filter on profile_id, either NULL or a bigint
        NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') IS NULL
      OR
        NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') RLIKE '(^"[0-9]*"$|^[0-9]*$)'
  )"""

print(f"""======================== Executing {datetime.now()} =======================""")
#print(stmt)
print("""=====================================================================================\n""")
df=spark.sql(stmt)
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Caching

# COMMAND ----------

source_query = r""" SELECT
  REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(e.val AS STRING),'/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003',' '),'\x01') AS val -- remove all linebreaks, returns, tabs, etc.
FROM prd_json.events e
where ds = '2022-06-13' and e.event_type = 'ND_ACTIVITY_START' and val like '%4ff68f5f-0244-41aa-b797-655b5e88513822061%' """
source_data = spark.sql(source_query);
source_data.cache()
source_data.count()
source_data.createOrReplaceTempView("sj_json_events")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sj_json_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC select REPLACE(REGEXP_REPLACE(REGEXP_REPLACE('{
# MAGIC     "body":
# MAGIC     {
# MAGIC         "activity_source":
# MAGIC         {
# MAGIC             "email_id": "jr09f8i0DdxFuy3ZBAYtVQrVJ9xHqYl0MDYpVEvf4urwok_QuM7zmVw80isCgWbb",
# MAGIC             "http_referrer": "https://nextdoor.co.uk/p/DTpYph6pCmr2?post=17592210118751&utm_source=email&is=tpe&section=cta&mar=true&ct=jr09f8i0DdxFuy3ZBAYtVQrVJ9xHqYl0MDYpVEvf4urwok_QuM7zmVw80isCgWbb&ec=OWKiQRDj9vEHAYwTV6YMARldwuFdgGkeefhwfGYAE0s%3D&mobile_deeplink_data=eyJhY3Rpb24iOiAidmlld19wb3N0IiwgInBvc3QiOiAxNzU5MjIxMDExODc1MX0%3D&link_source_user_id=17592189606588",
# MAGIC             "referral_source":
# MAGIC             {
# MAGIC                 "ct": "jr09f8i0DdxFuy3ZBAYtVQrVJ9xHqYl0MDYpVEvf4urwok_QuM7zmVw80isCgWbb",
# MAGIC                 "ec": "OWKiQRDj9vEHAYwTV6YMARldwuFdgGkeefhwfGYAE0s=",
# MAGIC                 "is": "tpe",
# MAGIC                 "link_source_user_id": "17592189606588",
# MAGIC                 "mar": "true",
# MAGIC                 "mobile_deeplink_data": "eyJhY3Rpb24iOiAidmlld19wb3N0IiwgInBvc3QiOiAxNzU5MjIxMDExODc1MX0=",
# MAGIC                 "post": "17592210118751",
# MAGIC                 "section": "cta",
# MAGIC                 "utm_source": "email"
# MAGIC             },
# MAGIC             "source": "email"
# MAGIC         },
# MAGIC         "client_info":
# MAGIC         {
# MAGIC             "app":
# MAGIC             {
# MAGIC                 "build_type": "prod",
# MAGIC                 "version": "15700harness-368cb6d327b5d764"
# MAGIC             },
# MAGIC             "platform":
# MAGIC             {
# MAGIC                 "name": "generic-web"
# MAGIC             },
# MAGIC             "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
# MAGIC             "web_experience": "desktop-web"
# MAGIC         },
# MAGIC         "device_identifiers":
# MAGIC         {
# MAGIC             "client_uid": "4ff68f5f-0244-41aa-b797-655b5e88513822061\u0001"
# MAGIC         },
# MAGIC         "host": "nextdoor.co.uk",
# MAGIC         "member_identifiers":
# MAGIC         {
# MAGIC             "cookies":
# MAGIC             {
# MAGIC                 "WE": "4ff68f5f-0244-41aa-b797-655b5e88513822061\u0001",
# MAGIC                 "WERC": "4db4a21d-5f5d-4f3b-a0b6-b1274dd0e026220613"
# MAGIC             },
# MAGIC             "user_profile_id": "17592189606588"
# MAGIC         },
# MAGIC         "nd_activity_id": "03DE005D-CA9D-4949-A7A6-85F1CB0053D3",
# MAGIC         "url":
# MAGIC         {
# MAGIC             "path": "/p/DTpYph6pCmr2",
# MAGIC             "query": "?post=17592210118751&utm_source=email&is=tpe&section=cta&mar=true&ct=jr09f8i0DdxFuy3ZBAYtVQrVJ9xHqYl0MDYpVEvf4urwok_QuM7zmVw80isCgWbb&ec=OWKiQRDj9vEHAYwTV6YMARldwuFdgGkeefhwfGYAE0s%3D&mobile_deeplink_data=eyJhY3Rpb24iOiAidmlld19wb3N0IiwgInBvc3QiOiAxNzU5MjIxMDExODc1MX0%3D&link_source_user_id=17592189606588"
# MAGIC         }
# MAGIC     },
# MAGIC     "epoch": 1655138928714,
# MAGIC     "name": "nd_activity_start",
# MAGIC     "nd_flask_version": "1",
# MAGIC     "remote_addr": "127.0.0.1:34978",
# MAGIC     "request":
# MAGIC     {
# MAGIC         "cookies":
# MAGIC         {},
# MAGIC         "protocol": "HTTP/1.1",
# MAGIC         "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"
# MAGIC     },
# MAGIC     "timestamp": 1655138928695
# MAGIC }','/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003',' '),'\u0001') AS val
# MAGIC from p_dwh_event.nd_activity_start
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC       REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(e.val AS STRING),'/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003',' '),'\u0001') AS val -- remove all linebreaks, returns, tabs, etc.
# MAGIC     FROM prd_json.events e
# MAGIC   where ds = '2022-06-13' and e.event_type = 'ND_ACTIVITY_START' and val like '%4ff68f5f-0244-41aa-b797-655b5e88513822061%';

# COMMAND ----------

# MAGIC %sql
# MAGIC select NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),'') AS client_uid
# MAGIC from (SELECT
# MAGIC       REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(e.val AS STRING),'/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003',' '),'\u0001') AS val -- remove all linebreaks, returns, tabs, etc.
# MAGIC     FROM prd_json.events e
# MAGIC   where ds = '2022-06-13' and e.event_type = 'ND_ACTIVITY_START' and val like '%4ff68f5f-0244-41aa-b797-655b5e88513822061%') as e;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Run 

# COMMAND ----------

from datetime import datetime

from pyspark.sql.session import SparkSession


def execute_statements():
  stmt = r"""CREATE TEMPORARY FUNCTION decrypt AS 'com.nextdoor.udf.NdCrypt'"""
  print(f"""======================== Executing {datetime.now()} =======================""")
  print(stmt)
  print("""=====================================================================================\n""")
  #spark.sql(stmt)
  stmt = r"""SELECT DISTINCT
  CAST(get_json_object(e.val, '$.timestamp') AS BIGINT) AS event_timestamp
  ,NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') AS user_profile_sk
  ,NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.cookies.WERC') AS STRING),'') AS session_cookie
  ,NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.cookies.WE') AS STRING),'') AS persistent_cookie
  ,ua.platform AS platform
  ,REGEXP_REPLACE(REGEXP_REPLACE(ua.user_agent, '\r', ''), '\n', ' ') AS user_agent -- no tabs or newlines
  ,ua.user_agent_category AS user_agent_category
  ,ua.device AS device
  ,ua.browser AS browser
  ,ua.browser_major AS browser_major
  ,ua.browser_minor AS browser_minor
  ,ua.os AS os
  ,ua.os_major AS os_major
  ,ua.os_minor AS os_minor
  ,ua.app_version AS app_version
  ,ua.app_version_major AS app_version_major
  ,ua.app_version_minor AS app_version_minor
  ,ua.app_version_patch AS app_version_patch
  ,DECRYPT(NULLIF(CAST(get_json_object(e.val, '$.body.activity_source.email_id') AS STRING),''), '[0-9]+') AS email_id
  ,NULLIF(CAST(get_json_object(e.val, '$.body.activity_source.source') AS STRING),'') AS nd_activity_source
  ,get_json_object(e.val, '$.body.activity_source') AS source_details
  ,NULLIF(CAST(get_json_object(e.val, '$.body.client_info.platform.name') AS STRING),'') AS platform_name
  ,NULLIF(CAST(get_json_object(e.val, '$.body.client_info.app.build_type') AS STRING),'') AS app_build_type
  ,NULLIF(CAST(get_json_object(e.val, '$.body.host') AS STRING),'') AS nd_site_host
  ,NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),'') AS client_uid
  ,NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),'') AS client_uid_hash
  ,NULLIF(UPPER(CAST(get_json_object(e.val, '$.body.nd_activity_id') AS STRING)),'') AS nd_activity_id
  ,'09' AS hr
  ,TRY_CAST(get_json_object(e.val, '$.body.activity_source.badge_count') AS INT) AS badge_count
FROM (
    SELECT
      REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(e.val AS STRING),'/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003',' '),'\u0001') AS val -- remove all linebreaks, returns, tabs, etc.
    FROM prd_json.events e
    where ds = '2022-06-13' and e.event_type = 'ND_ACTIVITY_START' and val like '%4ff68f5f-0244-41aa-b797-655b5e88513822061%'
) e
LEFT JOIN prd_avro.user_agent ua
  ON NULLIF(
        --REGEXP_REPLACE(REGEXP_REPLACE(
              COALESCE(
                    CAST(get_json_object(e.val, '$.request.user_agent') AS STRING)
                   ,CAST(get_json_object(e.val, '$.body.client_info.user_agent') AS STRING)
              )
        --      , '\r', ' '), '\n', ' ')
        ,'')
            = REGEXP_REPLACE(REGEXP_REPLACE(ua.user_agent, '\r', ' '), '\n', ' ')
  AND ua.ds = '2022-06-16'
  AND ua.hr = '09'
WHERE (  -- same as client_view filter on profile_id, either NULL or a bigint
            NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') IS NULL
          OR
            NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') RLIKE '(^"[0-9]*"$|^[0-9]*$)'
      )"""
  print(f"""======================== Executing {datetime.now()} =======================""")
  print(stmt)
  print("""=====================================================================================\n""")
  return spark.sql(stmt)

if __name__ == '__main__':
  result = execute_statements()
  settings = {'use_insert_overwrite': True}
  # Generated by DataframeWriterHelper: BEGIN
  import uuid
  from datetime import datetime, timedelta

  import pytz

  ESTIMATED_COLUMN_SIZE = 50  # bytes
  ESTIMATED_FILE_SIZE = 300000000  # bytes
  MAX_OUTPUT_PARTITIONS = 200


  def repartition_and_write(df, path, settings):
      if df is None:
          print("df is None")
          return
      cached_df = df.cache()  # so multiple df.* calls later will be faster

      output_partitions = settings.get('output_partitions')
      if output_partitions:
          cached_df = cached_df.repartition(output_partitions)
      else:
          number_of_columns = len(cached_df.schema.names)
          number_of_rows = cached_df.count()  # ACTION
          print(f"Number of rows: {number_of_rows}")
          estimated_partitions = int(number_of_rows * number_of_columns * ESTIMATED_COLUMN_SIZE / ESTIMATED_FILE_SIZE) + 1
          print(f"Estimated number of partitions: {str(estimated_partitions)}")
          output_partitions = min(estimated_partitions, MAX_OUTPUT_PARTITIONS)
          # repartition performs better than coalesce according to https://github.com/Nextdoor/dataflow-dags/pull/3643
          cached_df = cached_df.repartition(output_partitions)

      # do the .head() after all changed have been made so that Spark can hopefully cache it between .head and .write
      sample_rows = cached_df.head(20)  # ACTION
      if len(sample_rows) == 0:
          print("Dataset is empty")
          return

      print(f"""===================== Sample output ({datetime.now()}) =====================""")
      spark.createDataFrame(sample_rows, cached_df.schema).show(truncate=False)
      print(f"Debug this extract's data further until {datetime.now(tz=pytz.UTC) + timedelta(days=2)} with:\n"
            f">>> df = spark.read.option('sep', '\\001').csv('{path}')")

      # Write CSV with SQL Insert Overwrite
      use_insert_overwrite = settings.get('use_insert_overwrite', False)
      if use_insert_overwrite:
          view_name = str(uuid.uuid4()).replace("-", "_")
          cached_df.createOrReplaceTempView(view_name)
          spark.sql(f"""
            INSERT OVERWRITE DIRECTORY '{path}'
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\001'
              select * from {view_name}
          """)
      else:
          write_mode = settings.get('write_mode', 'overwrite')
          dfw = cached_df.write.mode(write_mode)  # DataFrame to DataFrameWriter

          partition_by = settings.get('partition_by', [])
          if partition_by:
              dfw = dfw.partitionBy(", ".join(["'{}'".format(col) for col in partition_by]))

          # default settings
          all_options = {
              'sep': '\001',
              'escape': '\000',
              'quote': '\000',
              'escapeQuotes': 'false',
              'nullValue': '\\N',
              'header': 'false'
          }
          options = settings.get('options', {})
          all_options.update(options)  # merge

          if all_options['escapeQuotes'] == 'true':
              del all_options['quote']
              del all_options['escape']

          for key, value in list(all_options.items()):
              dfw = dfw.option(key, value)
          dfw.csv(path)  # ACTION


  repartition_and_write(result, 's3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__core_events_hourly/nd_activity_start_pii/20220616T090000/', settings)


# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS x__prd_bridge.bridge__core_events_hourly_nd_activity_start_sj""")
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS x__prd_bridge.bridge__core_events_hourly_nd_activity_start_sj (
`event_timestamp` bigint,    `user_profile_sk` string,    `session_cookie` string,    `persistent_cookie` string,    `platform` int,    `user_agent` string,    `user_agent_category` string,    `device` string,    `browser` string,    `browser_major` string,    `browser_minor` string,    `os` string,    `os_major` string,    `os_minor` string,    `app_version` string,    `app_version_major` string,    `app_version_minor` string,    `app_version_patch` string,    `email_id` string,    `nd_activity_source` string,    `source_details` string,    `platform_name` string,    `app_build_type` string,    `nd_site_host` string,    `client_uid` string,    `client_uid_hash` string,    `nd_activity_id` string,    `hr` string,    `badge_count` int)
STORED AS TEXTFILE
LOCATION 's3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__core_events_hourly/nd_activity_start_pii/20220616T090000/'""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from x__prd_bridge.bridge__core_events_hourly_nd_activity_start_sj;

# COMMAND ----------

df=df = spark.read.option('sep', '\001').csv('s3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__core_events_hourly/nd_activity_start_pii/20220616T090000/')
df.show(100,False)

# COMMAND ----------

# tried - <0x01>, 
stmt = r"""CREATE TEMPORARY FUNCTION decrypt AS 'com.nextdoor.udf.NdCrypt'"""
print(f"""======================== Executing {datetime.now()} =======================""")
print(stmt)
print("""=====================================================================================\n""")
spark.sql(stmt)
stmt = r"""SELECT DISTINCT
REGEXP_REPLACE(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.cookies.WE') AS STRING),''), '\001','') AS persistent_cookie
,REGEXP_REPLACE(NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),''),'\001','') AS client_uid
,REGEXP_REPLACE(NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),''), '\001' ,'') AS client_uid_hash
,NULLIF(UPPER(CAST(get_json_object(e.val, '$.body.nd_activity_id') AS STRING)),'') AS nd_activity_id
,'09' AS hr
,TRY_CAST(get_json_object(e.val, '$.body.activity_source.badge_count') AS INT) AS badge_count
FROM (
SELECT
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(e.val AS STRING),'/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003',' '),'\001','') AS val -- remove all linebreaks, returns, tabs, etc.
FROM sj_json_events e
) e
LEFT JOIN prd_avro.user_agent ua
ON NULLIF(
    --REGEXP_REPLACE(REGEXP_REPLACE(
          COALESCE(
                CAST(get_json_object(e.val, '$.request.user_agent') AS STRING)
               ,CAST(get_json_object(e.val, '$.body.client_info.user_agent') AS STRING)
          )
    --      , '\r', ' '), '\n', ' ')
    ,'')
        = REGEXP_REPLACE(REGEXP_REPLACE(ua.user_agent, '\r', ' '), '\n', ' ')
AND ua.ds = '2022-06-16'
AND ua.hr = '09'
WHERE (  -- same as client_view filter on profile_id, either NULL or a bigint
        NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') IS NULL
      OR
        NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') RLIKE '(^"[0-9]*"$|^[0-9]*$)'
  )"""
print(f"""======================== Executing {datetime.now()} =======================""")
#print(stmt)
print("""=====================================================================================\n""")
df=spark.sql(stmt)
df.show(1000,False)

# COMMAND ----------

# tried - <0x01>, 
stmt = r"""CREATE TEMPORARY FUNCTION decrypt AS 'com.nextdoor.udf.NdCrypt'"""
print(f"""======================== Executing {datetime.now()} =======================""")
print(stmt)
print("""=====================================================================================\n""")
#spark.sql(stmt)
stmt = r"""SELECT DISTINCT
NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.cookies.WE') AS STRING),'') AS persistent_cookie
,NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),'') AS client_uid
,NULLIF(CAST(get_json_object(e.val, '$.body.device_identifiers.client_uid') AS STRING),'') AS client_uid_hash
,NULLIF(UPPER(CAST(get_json_object(e.val, '$.body.nd_activity_id') AS STRING)),'') AS nd_activity_id
,'09' AS hr
,TRY_CAST(get_json_object(e.val, '$.body.activity_source.badge_count') AS INT) AS badge_count
FROM (
SELECT
  REGEXP_REPLACE(REGEXP_REPLACE(CAST(e.val AS STRING),'/(\r\n)+|\r+|\n+|\t+/i',' '),'\u001|\u002|\u003|\001',' ') AS val -- remove all linebreaks, returns, tabs, etc.
FROM sj_json_events e
) e
LEFT JOIN prd_avro.user_agent ua
ON NULLIF(
    --REGEXP_REPLACE(REGEXP_REPLACE(
          COALESCE(
                CAST(get_json_object(e.val, '$.request.user_agent') AS STRING)
               ,CAST(get_json_object(e.val, '$.body.client_info.user_agent') AS STRING)
          )
    --      , '\r', ' '), '\n', ' ')
    ,'')
        = REGEXP_REPLACE(REGEXP_REPLACE(ua.user_agent, '\r', ' '), '\n', ' ')
AND ua.ds = '2022-06-16'
AND ua.hr = '09'
WHERE (  -- same as client_view filter on profile_id, either NULL or a bigint
        NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') IS NULL
      OR
        NULLIF(NULLIF(NULLIF(CAST(get_json_object(e.val, '$.body.member_identifiers.user_profile_id') AS STRING),''),'null'),'0') RLIKE '(^"[0-9]*"$|^[0-9]*$)'
  )"""
print(f"""======================== Executing {datetime.now()} =======================""")
#print(stmt)
print("""=====================================================================================\n""")
df=spark.sql(stmt)
df.show(100,False)

# COMMAND ----------


