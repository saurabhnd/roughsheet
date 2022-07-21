# Databricks notebook source
df = spark.sql("""
with ds2 as (
    select max(ds) as ds
    from p_dwh_features.user_product_interaction_v1 
    where ds > '2022-04-25'
    )

, user_product_interaction_v1 as (
    select user_product_interaction_v1.* 
    from p_dwh_features.user_product_interaction_v1
    inner join ds2
      on user_product_interaction_v1.ds = ds2.ds
    )

select gim.id as user_profile_id
     , home_owners
     , children_under_3
     , education_university_degree
     , english_setting
     , gender_female
     , paid_acquire
     , us_state_id
     , dma_id
     , has_contacts
     , has_profile_photo
     , is_lead
     , has_spouse_email
     , months_from_signup
     , signed_up_this_month
     , months_from_move
      
from user_product_interaction_v1 as pf
left join p_private.global_id_map gim
  on gim.sk = pf.user_profile_sk
  and gim.column_name = 'profile_sk'
""")

columns = ['user_profile_id', 'home_owners', 'children_under_3', 'education_university_degree',
           'english_setting', 'gender_female', 'paid_acquire', 'us_state_id', 'dma_id',
           'has_contacts', 'has_profile_photo', 'is_lead', 'has_spouse_email', 'months_from_signup',
           'signed_up_this_month', 'months_from_move']

df = df.select(*columns)
settings = { }

# COMMAND ----------

# Generated by DataframeWriterHelper: BEGIN
import uuid
from datetime import datetime
from pprint import pprint

ESTIMATED_COLUMN_SIZE = 50  # bytes
ESTIMATED_FILE_SIZE = 300000000  # bytes
MAX_OUTPUT_PARTITIONS = 200


def repartition_and_write(df, path, settings):
    cached_df = df.cache()  # so multiple df.* calls later will be faster

    if cached_df is None:
        print("df is None")
        return

    output_partitions = settings.get('output_partitions')
    if output_partitions:
        cached_df = cached_df.repartition(output_partitions)
    else:
        number_of_columns = len(cached_df.schema.names)
        number_of_rows = cached_df.count()  # ACTION
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
    pprint(sample_rows)
    # print(f"of {cached_df.count()} total rows")  # removing total count for now as it's expensive

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

        display(cached_df)

# COMMAND ----------

repartition_and_write(df, 's3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__features_1day/fs_user_generic/20220510T000000/', settings)


# COMMAND ----------

df=spark.read.text("s3://prd-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__features_1day/fs_user_generic/20220510T000000/")
display(df)

# COMMAND ----------

display(spark.sql(""" show databases"""))

# COMMAND ----------

import os

import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fs_user_generic.to_spark_csv")
spark = spark.config("spark.sql.files.maxRecordsPerFile", 700000)
spark = spark.enableHiveSupport().getOrCreate()

def get_s3_resource():
    if os.getenv('DATABRICKS_RUNTIME_VERSION'):  # TODO remove after full move to DBR
        return boto3.Session(profile_name='default').resource('s3')
    else:
        return boto3.resource('s3')

def set_acl(acl):
    s3 = get_s3_resource()
    paginator = s3.meta.client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket="dataflow-transfer-us1-nextdoor-com", Prefix="nextdoor.com/fs/user_generic"):
        for item in page['Contents']:
            obj = s3.Object("dataflow-transfer-us1-nextdoor-com", item['Key'])
            #obj.Acl().put(ACL=acl)

spark.sql("DROP TABLE IF EXISTS temp.fs_user_generic_20220510000000")
spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS temp.fs_user_generic_20220510000000 ( \
    `user_profile_id` string, \
    `home_owners` double, \
    `children_under_3` double, \
    `education_university_degree` double, \
    `english_setting` int, \
    `gender_female` int, \
    `paid_acquire` int, \
    `us_state_id` int, \
    `dma_id` int, \
    `has_contacts` int, \
    `has_profile_photo` int, \
    `is_lead` int, \
    `has_spouse_email` int, \
    `months_from_signup` int, \
    `signed_up_this_month` int, \
    `months_from_move` int \
) \
STORED AS TEXTFILE \
LOCATION 's3://prd-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/prd/bridge__features_1day/fs_user_generic/20220510T000000/'")
df = spark.sql("select * from temp.fs_user_generic_20220510000000")
(df.write
    .mode("overwrite")
    .format("csv")
    .option("ignoreNullFields", "False")
    .option("sep", "\u0001")
    .save("s3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/nextdoor.com/fs/user_generic_/"))
#spark.sql("DROP TABLE IF EXISTS temp.fs_user_generic_20220510000000")
set_acl("bucket-owner-full-control")
display(df)

# COMMAND ----------

df = spark.sql("select * from temp.fs_user_generic_20220510000000")

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.option("header","true").option("inferSchema","true").option("sep", '\u0001').csv("s3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/nextdoor.com/fs/user_generic/")
df_filtered = df.filter(col("user_profile_id") == '17592196088583')
display(df_filtered)

# COMMAND ----------

df=spark.read.option("header","true").option("inferSchema","true").option("sep", '\001').csv("s3://dev-dwhlatest-text.datalake.nextdoor.com/tmp__bridge/nextdoor.com/fs/user_generic/")
display(df)

# COMMAND ----------

df=spark.read.option("header","true").option("inferSchema","true").option("sep", '\u0001').csv("s3://dataflow-transfer-us1-nextdoor-com/nextdoor.com/fs/user_generic/")
display(df)

# COMMAND ----------

