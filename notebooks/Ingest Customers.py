# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, DoubleType, StructType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run ../scripts/scripts

# COMMAND ----------

file_path = "/data/Customer.xlsx"
absolute_file_path = get_absolute_file_path(file_path)

customersDFRaw = create_customers_raw_df(absolute_file_path)
customersDFRaw.count()

# COMMAND ----------

customersDFRaw.show()

# COMMAND ----------

customersDFRaw.groupBy("phone").count().where("count > 1").show()

# COMMAND ----------

customersDFRaw.groupBy("Region").count().where("count > 1").show()

# COMMAND ----------

write_to_parquet(customersDFRaw, "raw/customers_raw.parquet")

# COMMAND ----------

dbutils.fs.ls("/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/raw/")

# COMMAND ----------

table = "/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/raw/customers_raw.parquet"
customersDFEnriched = create_customers_enriched_df(table)
customersDFEnriched.show()

# COMMAND ----------

write_to_parquet(customersDFEnriched, "/enriched/customers_enriched.parquet")
dbutils.fs.ls("/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/enriched/")
