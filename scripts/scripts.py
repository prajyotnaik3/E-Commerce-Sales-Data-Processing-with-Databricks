# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, udf
from pyspark.sql.types import StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def get_absolute_file_path(path):
    return "file:/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks" + path

# COMMAND ----------

def read_products_csv(file_path, schema=None):
    return spark.read.format('csv').option('header', True).option("escape", '"').option("quote", '"').schema(schema).load(file_path)

# COMMAND ----------

def read_customers_xlsx(file_path, schema=None):
    return spark.read.format("com.crealytics.spark.excel").option("header", True).option("escape", '"').option("quote", '"').option("sheetName", "Worksheet").schema(schema).load(file_path)

# COMMAND ----------

def read_orders_json(file_path, schema, cols_mapping=None, cast_dates=None):
    df = spark.read.format("json").schema(schema).option("multiline", True).option("mode", "DROPMALFORMED").load(file_path)
    df = df.withColumnsRenamed(cols_mapping) if cols_mapping else df
    if cast_dates:
        for column in cast_dates:
            df = df.withColumn(column, to_date(col(column), "d/M/yyyy"))
    return df

# COMMAND ----------

def write_to_parquet(df, filename):
    df.write.mode("overwrite").parquet(f"/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/processed_data/{filename}")

# COMMAND ----------

def create_products_raw_df(file_path):
    return spark.read.format('csv').option('header', True).option("escape", '"').option("quote", '"').load(file_path)

# COMMAND ----------

def create_customers_raw_df(file_path):
    return spark.read.format("com.crealytics.spark.excel").option("header", True).option("escape", '"').option("quote", '"').option("sheetName", "Worksheet").load(file_path)

# COMMAND ----------

def create_orders_raw_df(file_path):
    return spark.read.format("json").option("multiline", True).option("mode", "DROPMALFORMED").load(file_path)

# COMMAND ----------

def create_products_enriched_df(table):
    cols_mapping = {"Product ID": "productID", "Category": "category", "Sub-Category": "subCategory", "Product Name": "productName", "State": "state", "Price per product": "pricePerProduct"}
    return spark.read.parquet(table).withColumnsRenamed(cols_mapping).withColumn("pricePerProduct", col("pricePerProduct").cast("double"))

# COMMAND ----------

def create_customers_enriched_df(table):
    cols_mapping = {"Customer ID": "customerID", "Customer Name": "customerName", "Segment": "segment", "Country": "country", "City": "city", "State": "state", "Postal Code": "postalCode", "Region": "region"}
    return spark.read.parquet(table).withColumnsRenamed(cols_mapping)

# COMMAND ----------

def clean_price(price):
    try:
        if price.endswith("%"):
            price = price[:-1]
        return price
    except Exception:
        return None

clean_price_udf = udf(clean_price, StringType())

# COMMAND ----------

def create_orders_enriched_df(table):
    cols_mapping = {'Row ID': 'rowID', 'Order ID': 'orderID', 'Order Date': 'orderDate', 'Ship Date': 'shipDate', 'Ship Mode': 'shipMode', 'Customer ID': 'customerID', 'Product ID': 'productID', 'Quantity': 'quantity', 'Price': 'price', 'Discount': 'discount', 'Profit':'profit'}
    return spark.read.parquet(table).withColumnsRenamed(cols_mapping).withColumn("orderDate", to_date(col("orderDate"), "d/M/yyyy")). withColumn("shipDate", to_date(col("shipDate"), "d/M/yyyy")).withColumn("price", clean_price_udf(col("price")).cast("double"))
