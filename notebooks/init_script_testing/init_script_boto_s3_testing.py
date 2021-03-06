# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
import boto3
import re

VALID_EVENT_REGEX = '[A-Za-z_]*$'

def get_events(bucket, prefix_to_scan):
    delimiter = ''
    session = boto3.session.Session(profile_name='default')
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator('list_objects')
    params = {'Bucket': bucket, 'Prefix': prefix_to_scan, 'Delimiter': delimiter}
    events = []
    for page in paginator.paginate(**params):
        if page and page.get('Contents'):
            for prefix in page.get('Contents'):
                key = prefix['Key']
                event = key.split('/')[-1]
                if re.match(VALID_EVENT_REGEX, event):
                    events.append(event)
    return events

def add_partitions(events, bucket, schema, table, version, ds, hr):
    for event in events:
        spark.sql("""
ALTER TABLE {schema}.{table} ADD IF NOT EXISTS
PARTITION (event_type = '{event}', v = '{version}', ds = '{ds}', hr = '{hr}')
LOCATION 's3://{bucket}/{event}/v={version}/ds={ds}/hr={hr}/'
""".format(schema=schema, table=table, event=event, version=version, ds=ds, hr=hr, bucket=bucket))

if __name__ == '__main__':
    spark = SparkSession.builder.appName("add_json_partitions").enableHiveSupport().getOrCreate()

    version = '001'
    bucket = 'prd-event-json.datalake.nextdoor.com'
    schema = 'dev_json'
    table = 'events'
    ds = '2022-05-06'
    hr = '21'

    prefix_to_scan = 'event_catalog/v={version}/ds={ds}/hr={hr}'.format(version=version, ds=ds, hr=hr)
    events = get_events(bucket, prefix_to_scan)
    
    print(len(events))
    
    #add_partitions(events, bucket, schema, table, version, ds, hr)
    #logging = [Row(key='events', value=",".join(events))]
    #df = spark.createDataFrame(logging)
    #df.show(10, False)
    # Generated by SparkOutputHelper: BEGIN

    from pyspark.sql.types import StringType
    from pyspark.sql.functions import col

    def rename_col_and_write(df, path):
        # rename columns with non-valid characters
        for index, name in enumerate(df.schema.names):
            df = df.withColumn('_col_' + str(index), col(name).cast(StringType()))
            df = df.drop(col(name))
        df.coalesce(1).write.mode('overwrite').format('ORC').save(path)

    #rename_col_and_write(df, 's3://prd-spark-logging-orc-datalake-analytics-nextdoor-com/data_lake__events_hourly/all.add_json_partitions/20220506210000/1')

# COMMAND ----------


