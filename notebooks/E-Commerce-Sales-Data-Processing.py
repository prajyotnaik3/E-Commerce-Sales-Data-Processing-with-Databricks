# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, avg, year, sum

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run ../scripts/E-Commerce-Sales-Scripts

# COMMAND ----------

products_raw = "ecommerce_sales.products_raw"
products_enriched = "ecommerce_sales.products_enriched"
customers_raw = "ecommerce_sales.customers_raw"
customers_enriched = "ecommerce_sales.customers_enriched"
orders_raw = "ecommerce_sales.orders_raw"
orders_enriched = "ecommerce_sales.orders_enriched"
orders_aggregated = "ecommerce_sales.orders_aggregated"

# COMMAND ----------

products_file_path = get_absolute_file_path("/data/Product.csv")
customers_file_path = get_absolute_file_path("/data/Customer.xlsx")
orders_file_path = get_absolute_file_path("/data/Order.json")

# COMMAND ----------

productsDFRaw = create_products_raw_df(products_file_path)
productsDFRaw.show()
create_delta_table(products_raw, productsDFRaw)

# COMMAND ----------

customersDFRaw = create_customers_raw_df(customers_file_path)
customersDFRaw.show()
create_delta_table(customers_raw, customersDFRaw)

# COMMAND ----------

ordersDFRaw = create_orders_raw_df(orders_file_path)
ordersDFRaw.show()
create_delta_table(orders_raw, ordersDFRaw)

# COMMAND ----------

productsDFEnriched = create_products_enriched_df(products_raw)
productsDFEnriched.show()
create_delta_table(products_enriched, productsDFEnriched)

# COMMAND ----------

customersDFEnriched = create_customers_enriched_df(customers_raw)
customersDFEnriched.show()
create_delta_table(customers_enriched, customersDFEnriched)

# COMMAND ----------

ordersDFEnriched = create_orders_enriched_df(products_enriched, customers_enriched ,orders_raw)
ordersDFEnriched.show()
create_delta_table(orders_enriched, ordersDFEnriched)

# COMMAND ----------

ordersDFAggregated = create_orders_aggregated_df(orders_enriched)
ordersDFAggregated.show()
create_delta_table(orders_aggregated, ordersDFAggregated)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, sum(profit) AS cumulative_profit
# MAGIC FROM ecommerce_sales.orders_aggregated
# MAGIC GROUP BY year
# MAGIC ORDER BY year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, category, sum(profit) AS cumulative_profit
# MAGIC FROM ecommerce_sales.orders_aggregated
# MAGIC WHERE category IS NOT NULL
# MAGIC GROUP BY year, category
# MAGIC ORDER BY year, category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customerID, sum(profit) AS cumulative_profit
# MAGIC FROM ecommerce_sales.orders_aggregated
# MAGIC GROUP BY customerID
# MAGIC ORDER BY cumulative_profit DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customerID, year, sum(profit) AS cumulative_profit
# MAGIC FROM ecommerce_sales.orders_aggregated
# MAGIC GROUP BY customerID, year
# MAGIC ORDER BY customerID, year
