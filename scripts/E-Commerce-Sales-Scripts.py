# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, udf, round, year, sum
from pyspark.sql.types import StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def get_absolute_file_path(path):
    """
    Creates absolute path to files in the git repo loaded in the workspace.
    Note: Repo name and account have fixed values.
    param path: string: file path relative to the root of the git repo
    Returns absolute file path on dbfs
    """
    return "file:/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks" + path

# COMMAND ----------

def create_delta_table(table, df):
    """
    Saves the DataFrame as a delta table. Creates the table if it doesn't exists. Overwrites the table data (as well as the schema) if table exists.
    param table: string: name for delta table
    param df: DataFrame: dataframe to be saved as delta table
    """
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)

# COMMAND ----------

def create_products_raw_df(file_path):
    """
    Reads and creates Spark Dataframe from Product.csv. Renames field names to follow camel case.
    param file_path: string: input file path for Product.csv 
    Returns raw products DataFrame
    """
    cols_mapping = {"Product ID": "productID", "Category": "category", "Sub-Category": "subCategory", "Product Name": "productName", "State": "state", "Price per product": "pricePerProduct"}
    return spark.read.format('csv') \
        .option('header', True) \
        .option("escape", '"') \
        .option("quote", '"') \
        .load(file_path) \
        .withColumnsRenamed(cols_mapping)

# COMMAND ----------

def create_products_enriched_df(table):
    """
    Reads and creates Spark Dataframe from raw delta table. Casts the pricePerProduct field to double.
    param table: string: delta table name for raw table to be enriched
    Returns enriched products DataFrame
    """
    return spark.table(table) \
        .withColumn("pricePerProduct", col("pricePerProduct").cast("double"))

# COMMAND ----------

def create_customers_raw_df(file_path):
    """
    Reads and creates Spark Dataframe from Customer.xlsx. Renames field names to follow camel case.
    param file_path: string: input file path for Customer.xlsx
    Returns raw customers DataFrame 
    """
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
    """
    Reads and creates Spark Dataframe from raw delta table. 
    TODO: Cleaning fields
    param table: string: delta table name for raw table to be enriched
    Returns enriched customers DataFrame
    """
    return spark.table(table)

# COMMAND ----------

def create_orders_raw_df(file_path):
    """
    Reads and creates Spark Dataframe from Order.json. Renames field names to follow camel case.
    param file_path: string: input file path for Order.json 
    Returns raw orders DataFrame
    """
    cols_mapping = {'Row ID': 'rowID', 'Order ID': 'orderID', 'Order Date': 'orderDate', 'Ship Date': 'shipDate', 'Ship Mode': 'shipMode', 'Customer ID': 'customerID', 'Product ID': 'productID', 'Quantity': 'quantity', 'Price': 'price', 'Discount': 'discount', 'Profit':'profit'}
    return spark.read.format("json") \
        .option("multiline", True) \
        .option("mode", "DROPMALFORMED") \
        .load(file_path) \
        .withColumnsRenamed(cols_mapping)

# COMMAND ----------


def clean_price(price):
    """
    Removes a trailing % from price string, if a % exists.
    param price: string: price string
    Returns cleaned price string
    """
    try:
        if price.endswith("%"):
            price = price[:-1]
        return price
    except Exception:
        return None

clean_price_udf = udf(clean_price, StringType())

# COMMAND ----------

def create_orders_enriched_df(products_table, customers_table, orders_table):
    """
    Reads and creates enriched Spark Dataframe from raw delta tables.
    param products_table: string: delta table name fore enriched products table
    param customers_table: string: delta table name fore enriched customers table
    param table: string: delta table name for raw table to be enriched
    Returns enriched orders DataFrame
    """
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
    """
    Creates aggregated spark DataFrame from enriched table, by grouping year, category, subCategory and customerID
    param table: string: enriched orders delta table to be aggregated
    Returns aggregated orders DataFrame
    """
    ordersDFEnriched = spark.table(table) \
        .withColumn("year", year(col("orderDate")))
    return ordersDFEnriched.groupBy(col("year"), col("category"), col("subCategory"), col("customerID")) \
        .agg(sum(col("profit")).alias("profit"))
