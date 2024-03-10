notebooks contain the Databricks notebbok used for data processing or ingesting the products, customers and orders data.

Notebook: E-Commerce-Sales-Data-Processing

As a part of running this notebook we create following delta tables:
* products_raw_table = "ecommerce_sales.products_raw"
* products_enriched_table = "ecommerce_sales.products_enriched"
* customers_raw_table = "ecommerce_sales.customers_raw"
* customers_enriched_table = "ecommerce_sales.customers_enriched"
* orders_raw_table = "ecommerce_sales.orders_raw"
* orders_enriched_table = "ecommerce_sales.orders_enriched"
* orders_aggregated_table = "ecommerce_sales.orders_aggregated"

Note that for tests we have following delta tables: 
* products_raw_table = "ecommerce_sales_test.products_raw"
* products_enriched_table = "ecommerce_sales_test.products_enriched"
* customers_raw_table = "ecommerce_sales_test.customers_raw"
* customers_enriched_table = "ecommerce_sales_test.customers_enriched"
* orders_raw_table = "ecommerce_sales_test.orders_raw"
* orders_enriched_table = "ecommerce_sales_test.orders_enriched"
* orders_aggregated_table = "ecommerce_sales_test.orders_aggregated"

We start by reading our raw csv file for products, Product.csv and creating a raw delta table. Next we create raw tables for customers and orders using Customer.xlsx and Order.json respectively.

Then we create an enriched products table to adhere to our required schema. Similarly, we create enriched customers table. 
To create enriched table for orders, we clean and transform raw orders table as well as use enriched products and customers tables to join and obtain th erequired columns.

Finally, we create an aggregate table by grouping the customers, by each product category and subCategory for each year and obtaining cumulative sum of profits. This aggregated table is then used to answer the necessary queries.




