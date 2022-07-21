# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
password = dbutils.secrets.get(scope="data-rds", key="rds-prd-gnarfeed_us")
driver = 'org.postgresql.Driver'
url = 'jdbc:postgresql://us1-gnarfeed-analytics.cluster-ro-cpeawz2ecqmz.us-east-1.rds.amazonaws.com:5432/gnarfeed?useSsl&serverSslCert=/tmp/rds-global-bundle.pem&tcpKeepAlive=true'
#url = 'jdbc:postgresql://eu1-gnarfeed-analytics.cluster-ro-cpeawz2ecqmz.us-east-1.rds.amazonaws.com/gnarfeed?useSsl&serverSslCert=/tmp/rds-global-bundle.pem&tcpKeepAlive=true'


# Get bounds. Use 0 to avoid NullPointerException if there is no data in the table (e.g. some regions dont have data)
bounds_query = "SELECT min(id) as lower_bound, max(id) as upper_bound FROM public.web_like"
bounds_df = spark.read \
    .format('jdbc') \
    .option('driver', driver) \
    .option('url', url) \
    .option('user', 'datalake') \
    .option('password', password) \
    .option('ssl', True) \
    .option('sslmode', 'require') \
    .option('query', bounds_query) \
    .load()
lower_bound = bounds_df.first().lower_bound or 0
upper_bound = bounds_df.first().upper_bound or 0

print(lower_bound, upper_bound)


# COMMAND ----------

# Extract
query = """(
SELECT  \"id\"::bigint AS \"id\",
     \"author_id\"::bigint AS \"author_id\",
     creation_date::character varying AS \"creation_date\",
     \"post_id\"::bigint AS \"post_id\",
     \"comment_id\"::bigint AS \"comment_id\",
     \"like_type\"::smallint AS \"like_type\",
     \"status\"::smallint AS \"status\",
     date_modified::character varying AS \"date_modified\",
     \"platform\"::smallint AS \"platform\",
     \"origination_group_id\"::bigint AS \"origination_group_id\",
     audit_cr_ts::character varying AS \"audit_cr_ts\",
     \"audit_version_num\"::integer AS \"audit_version_num\",
     audit_upd_ts::character varying AS \"audit_upd_ts\",
     \"reaction_type\"::smallint AS \"reaction_type\",
    extract(epoch from NOW())::bigint AS x__last_updated_ts,

    cast('update' as varchar) AS x__operation,cast(0 as varchar) AS x__lsn

    FROM public.web_like
) as subquery
"""

df = spark.read \
    .format('jdbc') \
    .option('driver', driver) \
    .option('url', url) \
    .option('user', 'datalake') \
    .option("dbtable", query) \
    .option("partitionColumn", "id") \
    .option("lowerBound", lower_bound) \
    .option("upperBound", upper_bound) \
    .option("numPartitions", 240) \
    .option("fetchsize", 1000) \
    .option('password', password) \
    .option('ssl', True) \
    .option('sslmode', 'require') \
    .load()

df.write \
    .mode('overwrite') \
    .format("com.databricks.spark.avro") \
    .save("s3://prd-dbs-raw-avro-datalake-analytics-nextdoor-com/gnarfeed__web_like_full/v=002/snapshot=2099051023/region=us")
