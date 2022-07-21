# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("{{ ti.task_id }}").enableHiveSupport().getOrCreate()

SKU_COST_MAP = {
    'PREMIUM_JOBS_COMPUTE': .096,
    'PREMIUM_ALL_PURPOSE_COMPUTE': .352,
    'PREMIUM_SQL_COMPUTE': .141,
    # POC acct was free-ish
    'ENTERPRISE_JOBS_COMPUTE': 0.0,
    'ENTERPRISE_SQL_COMPUTE': 0.0,
    'ENTERPRISE_ALL_PURPOSE_COMPUTE': 0.0,
}
EC2_COST_MAP = {
    'i3.xlarge': 0.312,  # on-demand
    'i3.2xlarge': 0.624,  # on-demand
    'r5d.2xlarge': 0.576,  # on-demand
    'r5d.large': 0.144,  # on-demand
    'r5dn.large': 0.167,  # on-demand
}
EC2_DEFAULT_COST = EC2_COST_MAP['i3.2xlarge']

WORKSPACE_MAP = {
    '2662743825661082': 'backfill',
    '333666217817422': 'production',
    '2277724437006607': 'staging',
    '1782801717446956': 'interactive',
    '49882262017806': 'test',
    '223006439975648': 'poc',
}

custom_tags_schema = StructType([
    StructField("dp_feature", StringType()),
    StructField("dag_id", StringType()),
    StructField("task_id", StringType()),
    StructField("initiative", StringType()),
    StructField("owner", StringType()),
    StructField("region", StringType()),
    StructField("dataset", StringType()),
    StructField("step", StringType()),
    StructField("sub_step", StringType()),
    StructField("user_id", StringType()),
])


@udf(returnType=FloatType())
def dbuCostUDF(dbus, sku):
    return float(dbus) * SKU_COST_MAP.get(sku, 0.0)


@udf(returnType=FloatType())
def machineCostUDF(machineHours, clusterNodeType):
    return float(machineHours) * float(EC2_COST_MAP.get(clusterNodeType, EC2_DEFAULT_COST))


@udf(returnType=StringType())
def workspaceUDF(workspaceId):
    return WORKSPACE_MAP.get(workspaceId, 'other')


# Read current month raw data
month = '{{ (execution_date).strftime("%Y-%m") }}'
print(month)
df = spark.sql(f"select * from p_dwh_general.dbr_cost_data_raw where month = '2022-05'")

# Load custom tags as json
df = df.withColumn("custom_tags", F.from_json(F.col('clusterCustomTags'), custom_tags_schema))
df = df.select('*', F.col('custom_tags.*'))

# Derive column data. Map workspaceId to workspace name, etc
df = (df
      .withColumn('dbu_cost', dbuCostUDF(F.col('dbus'), F.col('sku')))
      .withColumn('machine_cost', machineCostUDF(F.col('machineHours'), F.col('clusterNodeType')))
      .withColumn('total_cost', F.col('dbu_cost') + F.col('machine_cost'))
      .withColumn('event_date', F.col('timestamp')[0:10])
      .withColumn('cluster_owner_user_name', F.col('clusterOwnerUserName'))
      .withColumn('workspace', workspaceUDF(F.col('workspaceId')))
      # mutate job-765255928881337-run-71302872 to something more group-by-able
      .withColumn('cluster_name', F.when(F.col('clusterName').rlike('job.*run'), 'job_run').otherwise(F.col('clusterName')))
      )

# Derive cost_group
cost_group_mapping_df = spark.sql('select * from p_dwh_general.dataset_cost_group_mapping')
df = (df
      .join(cost_group_mapping_df, on=(df.dataset == cost_group_mapping_df.dataset), how='left')
      .drop(cost_group_mapping_df.dataset)
      )

df = df.select('event_date', 'workspace', 'cluster_owner_user_name', 'dbu_cost', 'machine_cost', 'total_cost',
               'dp_feature', 'dag_id', 'task_id', 'cost_group', 'owner', 'region', 'dataset', 'step', 'sub_step',
               'user_id','cluster_name')
df=df.filter(F.col("workspace") == 'interactive')
display(df.select("cluster_owner_user_name","cluster_name"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct month from p_dwh_general.dbr_cost_data_raw limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from p_dwh_general.dbr_cost_data where workspace = 'interactive' and cluster_name is not null order by event_date desc limit 10;

# COMMAND ----------


