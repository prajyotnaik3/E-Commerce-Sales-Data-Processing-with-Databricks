# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, udf, round, year, sum
from pyspark.sql.types import StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def get_absolute_file_path(path):
    return "file:/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks" + path

# COMMAND ----------

def create_delta_table(table, df):
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)

# COMMAND ----------

def create_products_raw_df(file_path):
    cols_mapping = {"Product ID": "productID", "Category": "category", "Sub-Category": "subCategory", "Product Name": "productName", "State": "state", "Price per product": "pricePerProduct"}
    return spark.read.format('csv') \
        .option('header', True) \
        .option("escape", '"') \
        .option("quote", '"') \
        .load(file_path) \
        .withColumnsRenamed(cols_mapping)

# COMMAND ----------

def create_products_enriched_df(table):
    return spark.table(table) \
        .withColumn("pricePerProduct", col("pricePerProduct").cast("double"))

# COMMAND ----------

def create_customers_raw_df(file_path):
    cols_mapping = {"Customer ID": "customerID", "Customer Name": "customerName", "Segment": "segment", "Country": "country", "City": "city", "State": "state", "Postal Code": "postalCode", "Region": "region"}
    return spark.read.format("com.crealytics.spark.excel") \
        .option("header", True) \
        .option("escape", '"') \
        .option("quote", '"') \
        .option("sheetName", "Worksheet") \
        .load(file_path) \
        .withColumnsRenamed(cols_mapping)

# COMMAND ----------

def create_customers_enriched_df(table):
    return spark.table(table)

# COMMAND ----------

def create_orders_raw_df(file_path):
    cols_mapping = {'Row ID': 'rowID', 'Order ID': 'orderID', 'Order Date': 'orderDate', 'Ship Date': 'shipDate', 'Ship Mode': 'shipMode', 'Customer ID': 'customerID', 'Product ID': 'productID', 'Quantity': 'quantity', 'Price': 'price', 'Discount': 'discount', 'Profit':'profit'}
    return spark.read.format("json") \
        .option("multiline", True) \
        .option("mode", "DROPMALFORMED") \
        .load(file_path) \
        .withColumnsRenamed(cols_mapping)

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

def create_orders_enriched_df(products_table, customers_table, orders_table):
    productsDFEnriched = spark.table(products_table) \
        .select(col("productID"), col("category"), col("subCategory")) \
        .distinct()
    customersDFEnriched = spark.table(customers_table) \
        .select(col("customerID"), col("customerName"), col("country"))
    ordersDF = spark.table(orders_table) \
        .withColumn("orderDate", to_date(col("orderDate"), "d/M/yyyy")) \
        .withColumn("shipDate", to_date(col("shipDate"), "d/M/yyyy")) \
        .withColumn("price", round(clean_price_udf(col("price")).cast("double"),2))
    return ordersDF.join(customersDFEnriched, on=["customerID"], how="inner") \
        .join(productsDFEnriched, on=["productID"], how="left")

# COMMAND ----------

def create_orders_aggregated_df(table):
    ordersDFEnriched = spark.table(table) \
        .withColumn("year", year(col("orderDate")))
    return ordersDFEnriched.groupBy(col("year"), col("category"), col("subCategory"), col("customerID")) \
        .agg(sum(col("profit")).alias("profit"))
