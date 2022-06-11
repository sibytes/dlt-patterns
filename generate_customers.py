# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from collections import OrderedDict
from faker import Faker
import datetime
import json
import os


# COMMAND ----------

import discover_modules
discover_modules.go(spark)

from utilities import (
  try_copy,
  try_rm,
  load_schema,
  save_schema
)

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

partition_path = "*/*/*/*.json"
raw_path = f"{path}/{partition_path}"
config = {
  "inferSchema": True
}
df = spark.read.format("json").options(**config).load(raw_path)
save_schema(df.schema, schema_root, stage, schema_name, dataset_name)

# COMMAND ----------

schema = load_schema(stage, schema_name, dataset_name)
config = {
  "inferSchema": False
}

df = spark.read.format("json").schema(schema).options(**config).load(raw_path)
display(df)

# COMMAND ----------

assert df.count() == 100
