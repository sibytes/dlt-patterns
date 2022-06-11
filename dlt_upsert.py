# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, os, json

sys.path.append("/Workspace/Repos/shaun.ryan@shaunchiburihotmail.onmicrosoft.com/dlt-patterns/utilities")


# COMMAND ----------

from utilities import load_schema

# COMMAND ----------

schema_name = "sibytes"
dataset_name = "customer"
stage = "raw"
lake_root = "/mnt/datalake"
delta_lake_root = "/mnt/datalake/delta"
schema_root = f"{lake_root}/schema"
path = f"{lake_root}/{stage}/{schema_name}/{dataset_name}"
to_path = f"{delta_lake_root}/{stage}/{schema_name}/{dataset_name}"

raw_path = f"{path}/*/*/*/"

print(
f"""
  schema_name: {schema_name}
  dataset_name: {dataset_name}
  stage: {stage}
  lake_root: {lake_root}
  delta_lake_root: {delta_lake_root}
  schema_root: {schema_root}
  path: {path}
  to_path: {to_path}
  raw_path: {raw_path}
"""
)

# COMMAND ----------


def get_raw_sibytes_customer(schema:StructType):
  
  partition = spark.conf.get(f"pipeline.{schema_name}.partition", default="*/*/*")
  raw_path = f"{path}/{partition}/*.json"
  
  config = {
    "inferSchema": False
  }
  df = (
     spark.readStream
    .format("json")
    .schema(schema)
    .options(**config)
    .load(raw_path)
    .withColumn("_SOURCE", input_file_name())
    .withColumn("_LOAD_DATE", expr("now()"))
  )
  return df

# COMMAND ----------

# _schema = load_schema(stage, schema_name, dataset_name)
# display(get_raw_sibytes_customer(_schema))

# COMMAND ----------

import dlt

@dlt.view(
  name=f"v{dataset_name}"
)
def raw_sibytes_customer():
  _schema = load_schema(stage, schema_name, dataset_name)
  return get_raw_sibytes_customer(_schema)


dlt.create_target_table(dataset_name, path = to_path,)

dlt.apply_changes(
  target = dataset_name,
  source = f"v{dataset_name}",
  keys = ["id"],
  sequence_by = col("modified")
)


