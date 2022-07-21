# Databricks notebook source
df=spark.sql(""" select * from p_private.pii limit 10""")
display(df)

# COMMAND ----------

df=spark.sql(""" select * from d_private.pii limit 10""")
display(df)

# COMMAND ----------

src = spark.read.orc(
        "s3://prd-pubsub-raw-orc-datalake-analytics-nextdoor-com/account__samwise__account/v=002/arrivaltime=2022042218/region=prd")

src.count()

# COMMAND ----------

src = spark.read.orc("s3://prd-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__core_events_hourly/request_tracking_pii/20220502T080000/")  ## default to ORC

# COMMAND ----------

spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS x__prd_bridge.bridge__core_events_hourly_request_tracking_20220505020000 (
`user_profile_sk` string,    `we_cookie_value` string,    `session_cookie_value` string,    `duration_microseconds` int,    `event_timestamp` bigint,    `url_method` string,    `url_domain` string,    `referer_domain` string,    `native_agent` string,    `platform` int,    `user_agent` string,    `user_agent_category` string,    `device` string,    `browser` string,    `browser_major` string,    `browser_minor` string,    `os` string,    `os_major` string,    `os_minor` string,    `request_identifier` string,    `valid_page` boolean,    `load_id` bigint,    `url_path_mute_request` boolean,    `url_path_promotion_server` boolean,    `http_code` int,    `url_utm_source` string,    `url_utm_medium` string,    `url_utm_campaign` string,    `url_utm_content` string,    `hr` string,    `ip_address` string,    `url_path_shared_post` boolean,    `share_id` string,    `is_background` boolean,    `app_version` string,    `app_version_major` string,    `app_version_minor` string,    `app_version_patch` string,    `device_id` string,    `url_route` string,    `nd_activity_id` string,    `nd_activity_source` string)
STORED AS TEXTFILE
LOCATION 's3://prd-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__core_events_hourly/request_tracking_pii/20220505T020000/'""")
src = spark.sql("SELECT * FROM x__prd_bridge.bridge__core_events_hourly_request_tracking_20220505020000")
if len(src.take(1)) == 0:
    print("No data is found. Exiting as a successful run!")
    logging_dict['no_data_is_found'] = str(True)
    dbutils.notebook.exit("No data is found. Exiting as a successful run!")
src.count()

# COMMAND ----------

top_secret = dbutils.secrets.get(scope="data-rds", key="metastore-db-pwd")
print(len(top_secret))
print(len(top_secret[0]))
top_secret = dbutils.secrets.getBytes(scope="data-rds", key="metastore-db-pwd")
#top_secret = dbutils.secrets.getBytes(scope="data-rds", key="surrogate-key-db-prd")

print(top_secret)


# COMMAND ----------

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://dev-scratch.datalake.nextdoor.com/u__saurabh/databricks_processed/init_script_metrics/init_script_optimization.csv")
df_select = df.select("execution_date","total_ec2_cost")
display(df_select)

# COMMAND ----------


