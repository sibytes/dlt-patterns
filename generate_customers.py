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

import datetime
now = datetime.datetime.now()
now.second
now.hour
now.microsecond

dte = datetime.datetime(year, month, 1, now.hour, now.minute, now.second, now.microsecond)


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
    "address_city": fake.city(),
    "address_country": fake.country(),
    "address_postcode": fake.postcode(),
    "address_street": fake.street_name(),
    "created": "",
    "modified": ""
  }
  
def customer_change_address(customer:dict, period:datetime):
  now = datetime.datetime.now()
  now.second
  now.hour
  now.microsecond
  customer["address_no"] = fake.building_number()
  customer["address_city"] = fake.city()
  customer["address_country"] = fake.country()
  customer["address_postcode"] = fake.postcode()
  customer["address_street"] = fake.street_name()
  customer["modified"] = str(datetime.datetime(period.year, period.month, period.day, now.hour, now.minute, now.second, now.microsecond))
  return customer

def customer_change_contact(customer:dict, period:datetime):
  now = datetime.datetime.now()
  now.second
  now.hour
  now.microsecond
  customer["contact_phone"] = fake.phone_number()
  customer["contact_email"] = fake.email()
  customer["modified"] = str(datetime.datetime(period.year, period.month, period.day, now.hour, now.minute, now.second, now.microsecond))
  return customer

records = [fake_customer(i) for i in range(total)]


now = datetime.datetime.now()
now.second
now.hour
now.microsecond


def to_json_line(r:dict, day:int=-1):
  
  if day>-1:
    dte_str = str(datetime.datetime(year, month, day, now.hour, now.minute, now.second, now.microsecond))
    r["created"] = dte_str
    r["modified"] = dte_str
  
  return "%s\n" % f"{json.dumps(r)}"

files = {}
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

    f.writelines(to_json_line(r, day) for r in record_set)
    
  files[i+1] = { "file": file_name, "period": period }
  

# insert modifications
for k,v in files.items():
  if k > 1:
    with open(files[k-1]["file"], "r") as pf:
      records = pf.readlines()
      record_change1 = json.loads(records[0])
      record_change1 = customer_change_address(record_change1, files[k]["period"])
      
      record_change2 = json.loads(records[1])
      record_change2 = customer_change_contact(record_change2, files[k]["period"])
      
      with open(files[k]["file"], "a+") as cf:
        cf.write(to_json_line(record_change1))
        cf.write(to_json_line(record_change2))

      

try_copy(path)


# COMMAND ----------

from pyspark.sql.functions import *

partition_path = "*/*/*/*.json"
raw_path = f"{path}/{partition_path}"
config = {
  "inferSchema": True
}
df = spark.read.format("json").options(**config).load(raw_path)
df = (df
      .withColumn("created", expr("cast(created as timestamp)"))
      .withColumn("modified", expr("cast(modified as timestamp)"))
     )

save_schema(df.schema, schema_root, stage, schema_name, dataset_name)


# COMMAND ----------

schema = load_schema(stage, schema_name, dataset_name)
config = {
  "inferSchema": False
}

df = (
  spark.read
  .format("json")
  .schema(schema)
  .options(**config)
  .load(raw_path)
  .withColumn("_SOURCE", input_file_name())
  .orderBy(col("id"))
)

display(df)

# COMMAND ----------

assert df.count() == 118
