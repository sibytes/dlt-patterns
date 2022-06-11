# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from collections import OrderedDict
from faker import Faker
import datetime
import json
import os

# COMMAND ----------

from pyspark.sql.types import StructType

def try_copy(path:str):
  
  try:
    dbutils.fs.rm(path, True)
  except:
    print(f"{path} doesn't exist")

  dbutils.fs.cp(f"file:{path}", path, True)

  try:
    dbutils.fs.rm(f"file:{path}", True)
  except:
    print(f"file:{path} doesn't exist")

    
def try_rm(path:str):
  
  try:
    dbutils.fs.rm(path, True)
  except:
    print(f"Path {path} does not exist.")
    
def load_schema(stage:str, schema_name:str, dataset_name:str):
  
  options = {"multiline": True,
            "lineSep": "~",
             "sep": "~"
            }
  path = f"/mnt/datalake/schema/{stage}.{schema_name}/spark.{stage}.{schema_name}.{dataset_name}.json"
  print(f"Loading schema from {path}")
  
  df = spark.read.format("csv").options(**options).load(path)
  json_schema = json.loads(df.first()[0])
  json_schema = StructType.fromJson(json_schema)
  
  print("Schema Loaded")
  print(json_schema)

  return json_schema





# COMMAND ----------


schema_name = "sibytes"
dataset_name = "customer"
stage = "raw"
lake_root = "/mnt/datalake"
schema_root = f"{lake_root}/schema"
path = f"{lake_root}/{stage}/{schema_name}/{dataset_name}"
total = 100
year = 2022
month = 1


# COMMAND ----------

locales = OrderedDict([
    ('en-US', 1)
])
fake = Faker(locales)


fake.seed_instance(0)
fake.seed_locale('en_US', 0)

def fake_customer(id:int):
  return {
    "id": id,
    "title": fake.prefix(),
    "first_name": fake.first_name(),
    "last_name": fake.last_name(),
    "dob": fake.date_of_birth().strftime("%Y-%m-%d"),
    "job": fake.job(),
    "contact_phone": fake.phone_number(),
    "contact_email": fake.email(),
    "address_no": fake.building_number(),
    "city": fake.city(),
    "country": fake.country(),
    "postcode": fake.postcode(),
    "street": fake.street_name()
  }

records = [fake_customer(i) for i in range(total)]


for i in range(10):
  l = (i*10)
  u = ((i+1)*10)
  print(f"records: {l} -> {u}")
  record_set = records[l:u]
  
  
  
  day = i+1
  period = datetime.date(day=i+1,month=month,year=year)
  period_name = period.strftime("%Y%m%d")
  period_path = period.strftime("%Y/%m/%d")
  to_path = f"{path}/{period_path}"
  file_name = f"{to_path}/{schema_name}_{dataset_name}_{period_name}.json"
  print(f"writing record_set: n = {len(record_set)} to {file_name}")
  os.makedirs(to_path, exist_ok=True)
  
  with open(file_name, "w") as f:

    jsonlns = [f"{json.dumps(j)}" for j in record_set]
    f.writelines("%s\n" % l for l in jsonlns)
  



try_copy(path)


# COMMAND ----------

raw_path = f"{path}/*/*/*/*.json"
config = {
  "inferSchema": True
}
df = spark.read.format("json").options(**config).load(raw_path)
display(df)

# COMMAND ----------

schema_path = f"{schema_root}/{stage}.{schema_name}"
schema_file = f"{schema_path}/spark.{stage}.{schema_name}.{dataset_name}.json"


try_rm(f"file:{schema_path}")
  
os.makedirs(schema_path, True)

schema_json = json.loads(df.schema.json())
schema = json.dumps(schema_json, indent=4)
with open(schema_file, "w") as f:
  f.write(schema)

try_copy(schema_file)

# COMMAND ----------

schema = load_schema(stage, schema_name, dataset_name)

raw_path = f"{path}/*/*/*/*.json"
config = {
  "inferSchema": False
}

df = spark.read.format("json").schema(schema).options(**config).load(raw_path)
display(df)

# COMMAND ----------

assert df.count() == 100
