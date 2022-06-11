from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import os
import json

_spark = SparkSession.builder.getOrCreate()
_dbutils = DBUtils(_spark)


def try_copy(path:str):
  
  try:
    _dbutils.fs.rm(path, True)
  except:
    print(f"{path} doesn't exist")

  _dbutils.fs.cp(f"file:{path}", path, True)

  try:
    _dbutils.fs.rm(f"file:{path}", True)
  except:
    print(f"file:{path} doesn't exist")

    
def try_rm(path:str):
  
  try:
    _dbutils.fs.rm(path, True)
  except:
    print(f"Path {path} does not exist.")
    
def load_schema(stage:str, schema_name:str, dataset_name:str):
  
  options = {"multiline": True,
            "lineSep": "~",
             "sep": "~"
            }
  path = f"/mnt/datalake/schema/{stage}.{schema_name}/spark.{stage}.{schema_name}.{dataset_name}.json"
  print(f"Loading schema from {path}")
  
  df = _spark.read.format("csv").options(**options).load(path)
  json_schema = json.loads(df.first()[0])
  json_schema = StructType.fromJson(json_schema)
  
  print("Schema Loaded")
  print(json_schema)

  return json_schema



def save_schema(schema:StructType, schema_root:str, stage:str, schema_name:str, dataset_name:str):
  
  schema_path = f"{schema_root}/{stage}.{schema_name}"
  schema_file = f"{schema_path}/spark.{stage}.{schema_name}.{dataset_name}.json"


  try_rm(f"file:{schema_path}")

  os.makedirs(schema_path, True)

  schema_json = json.loads(schema.json())
  schema = json.dumps(schema_json, indent=4)
  with open(schema_file, "w") as f:
    f.write(schema)

  try_copy(schema_file)