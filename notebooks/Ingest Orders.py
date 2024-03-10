# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, DoubleType, StructType, IntegerType, DateType
from pyspark.sql.functions import col, to_date

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run ../scripts/scripts

# COMMAND ----------

file_path = get_absolute_file_path("/data/Order.json")

ordersSchema = StructType([
    StructField("Row ID", IntegerType(), False),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])
cols_mapping = {"Row ID": "rowID", "Order ID": "orderID", "Order Date": "orderDate", "Ship Date": "shipDate", "Ship Mode": "shipMode", "Customer ID": "customerID", "Product ID": "productID", "Quantity": "quantity", "Price": "price", "Discount": "discount", "Profit": "profit"}
cast_dates = ["orderDate", "shipDate"]

# COMMAND ----------

ordersDFRaw = create_orders_raw_df(file_path)
ordersDFRaw.dtypes

# COMMAND ----------

ordersDFRaw.show()

# COMMAND ----------

write_to_parquet(ordersDFRaw, "raw/orders_raw.parquet")
dbutils.fs.ls("/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/raw/")

# COMMAND ----------

table = "/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/raw/orders_raw.parquet"
ordersDFEnriched = create_orders_enriched_df(table)
ordersDFEnriched.show()

# COMMAND ----------

write_to_parquet(ordersDFEnriched, "/enriched/orders_enriched.parquet")
dbutils.fs.ls("/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/enriched/")
