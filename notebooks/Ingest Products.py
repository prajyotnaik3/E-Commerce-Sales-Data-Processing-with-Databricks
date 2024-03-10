# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, DoubleType, StructType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run ../scripts/scripts

# COMMAND ----------

file_path = "/data/Product.csv"
absolute_file_path = get_absolute_file_path(file_path)

productsDFRaw = create_products_raw_df(absolute_file_path)
productsDFRaw.count()

# COMMAND ----------

productsDFRaw.show()

# COMMAND ----------

productsDFRaw.where('"Price per product" IS NULL').count()

# COMMAND ----------

productsDFRaw.filter(productsDFRaw["Product ID"].isNull()).count()
productsDFRaw.filter(productsDFRaw["Category"].isNull()).count()
productsDFRaw.filter(productsDFRaw["Sub-Category"].isNull()).count()
productsDFRaw.filter(productsDFRaw["Product Name"].isNull()).count()
productsDFRaw.filter(productsDFRaw["State"].isNull()).count()
productsDFRaw.filter(productsDFRaw["Price per product"].isNull()).count()

# COMMAND ----------

write_to_parquet(productsDFRaw, "raw/products_raw.parquet")

# COMMAND ----------

schema = StructType([
    StructField("productID", StringType(), True), 
    StructField("category", StringType(), True),
    StructField("subCategory", StringType(), True),
    StructField("productName", StringType(), True),
    StructField("state", StringType(), True),
    StructField("pricePerProduct", DoubleType(), True) 
])
dddf = spark.read.format('csv').option('header', True).option("escape", '"').option("quote", '"').option("inferSchema", True).schema(schema).load(get_absolute_file_path(file_path))

# COMMAND ----------

# Code to save dddf dataframe as a table "products_raw_test"
dddf.write.format("delta").mode("overwrite").saveAsTable("ecommerce_sales.products_raw")

# Code to read from the table into a dataframe dddf1
dddf1 = spark.table("ecommerce_sales.products_raw")

dddf1.show()

# COMMAND ----------



# COMMAND ----------

dddf1 = spark.table("default.products_raw_test")

# COMMAND ----------

dddf1.show()

# COMMAND ----------

prodDF = spark.read.format("delta").load("default.products_raw_test")
prodDF.show()

# COMMAND ----------

dbutils.fs.ls("/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/raw/")

# COMMAND ----------

table = "/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/raw/products_raw.parquet"
productsDFEnriched = create_products_enriched_df(table)
productsDFEnriched.show()

# COMMAND ----------

write_to_parquet(productsDFEnriched, "/enriched/products_enriched.parquet")
dbutils.fs.ls("/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/enriched/")

# COMMAND ----------


