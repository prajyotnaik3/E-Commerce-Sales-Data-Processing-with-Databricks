# E-Commerce-Sales-Data-Processing-with-Databricks
E-commerce Sales Data Processing with Databricks has the following structure:

| --------- data: contains input data files

| --------- docs: contains additinonal documentation including databricks notebooks in ipynb to capture the outputs of the notebooks

| --------- notebooks: contains Databricks notebooks used for processing data

| --------- scripts: contains script functions used for processing the data

| --------- tests: contains test files used for testing the script functions

| --------- requirements.txt: contains requirements

| --------- README.md: documents the details of this repository and runs

#### data 
data directory contains three input files:

1) Product.csv
2) Customer.xlsx
3) Order.json

#### notebooks
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

For outputs refer to ipynb notebooks under docs directory.

#### docs
docs directory contains the ipynb notebooks along with outputs:

1) [E-Commerce-Sales-Scripts.ipynb](https://github.com/prajyotnaik3/E-Commerce-Sales-Data-Processing-with-Databricks/blob/main/docs/E-Commerce-Sales-Scripts.ipynb)
2) [tests-products.ipynb](https://github.com/prajyotnaik3/E-Commerce-Sales-Data-Processing-with-Databricks/blob/main/docs/tests-products.ipynb)
3) [tests-customers.ipynb](https://github.com/prajyotnaik3/E-Commerce-Sales-Data-Processing-with-Databricks/blob/main/docs/tests-customer.ipynb)
4) [tests-orders.ipynb](https://github.com/prajyotnaik3/E-Commerce-Sales-Data-Processing-with-Databricks/blob/main/docs/tests-orders.ipynb)
5) [E-Commerce-Sales-Data-Processing.ipynb](https://github.com/prajyotnaik3/E-Commerce-Sales-Data-Processing-with-Databricks/blob/main/docs/E-Commerce-Sales-Data-Processing.ipynb)


#### scripts
[E-Commerce-Sales-Scripts] scripts contains the functions used to process the e-commerce sales data using Databricks

1) get_absolute_file_path: Creates absolute path to files in the git repo loaded in the workspace.
    * Note: Repo name and account have fixed values for my account.
    * param path: string: file path relative to the root of the git repo
    * Returns absolute file path on dbfs
    * This piece of functionality is required as we can't directly access the files inside the repo while creating dataframes
2) create_delta_table: Saves the DataFrame as a delta table. Creates the table if it doesn't exists. Overwrites the table data (as well as the schema) if table exists.
    * param table: string: name for delta table
    * param df: DataFrame: dataframe to be saved as delta table
    * This function has been used throughout to create raw, enriched and aggregagted delta tables.
3) create_products_raw_df: Reads and creates raw Spark Dataframe from Product.csv. Renames field names to follow camel case.
    * param file_path: string: input file path for Product.csv 
    * Returns raw products DataFrame
    * Function handles field values that contain commas and thus inside quotes by escaping quotes(").
    * Function renames the columns (which would ideally be done in enriched stage), so that column names do not contain any spaces as required by column names for delta tables.
4) create_products_enriched_df: Reads and creates Spark Dataframe from raw delta table. Casts the pricePerProduct field to double.
    * param table: string: delta table name for raw table to be enriched
    * Returns enriched products DataFrame
5) create_customers_raw_df: Reads and creates Spark Dataframe from Customer.xlsx. Renames field names to follow camel case.
    * param file_path: string: input file path for Customer.xlsx
    * Returns raw customers DataFrame 
    * Function handles field values that contain commas and thus inside quotes by escaping quotes(").
    * Function renames the columns (which would ideally be done in enriched stage), so that column names do not contain any spaces as required by column names for delta tables.
6) create_customers_enriched_df: Reads and creates Spark Dataframe from raw delta table. 
    * TODO: Cleaning fields
    * param table: string: delta table name for raw table to be enriched
    * Returns enriched customers DataFrame
    * Columns haven't been cleaned. Folowing needs to be handled:
      * customerName contains multiple whitespaces, digits and special characters as well empty values.
      * phone contains values "#ERROR!" and some negative integer values that need to be replaced with null
      * phone numbers do not follow a unique format and need to be updated to follow format like E.164 format that allows phone number and extension numbers in forms like (555)555-5555x55555
      * postal code contains entries that are not 5 digit long or begin with a 0
7) create_orders_raw_df: Reads and creates raw Spark Dataframe from Order.json. Renames field names to follow camel case.
    * param file_path: string: input file path for Order.json 
    * Returns raw orders DataFrame
    * Handles the multiline json records and drops any malformed records (rows)
    * Function renames the columns (which would ideally be done in enriched stage), so that column names do not contain any spaces as required by column names for delta tables.
8) clean_price: Removes a trailing % from price string, if a % exists.
    * param price: string: price string
    * Returns cleaned price string
    * This is used as a user defined function created to clean price column that can contain trailing % in the price value, and thus can't be simply casted to double. 
9) create_orders_enriched_df: Reads and creates enriched Spark Dataframe from raw delta tables.
    * param products_table: string: delta table name fore enriched products table
    * param customers_table: string: delta table name fore enriched customers table
    * param table: string: delta table name for raw table to be enriched
    * Returns enriched orders DataFrame
    * This function selects required columns from enriched products and customers tablea (DFs) and joins with then orders raw table to create enriched orders table. 
    * It formates the date column or orders raw table using format d/M/yyyy to parse the string orderDate and shipDate columns as date types. 
    * It joins intermediate orders table with enriched customers table using customerID column
    * It joins the result of the above join with the enriched productes table using productID column with a left join to keep all orders even if some of the orders productID are missing in products enriched table.
10) create_orders_aggregated_df: Creates aggregated spark DataFrame from enriched table, by grouping year, category, subCategory and customerID
    * param table: string: enriched orders delta table to be aggregated
    * Returns aggregated orders DataFrame
    * This dataframe holds profit for each customerID, product category and subCategory in a given year.



#### tests
tests directory contains test cases written testing ingestion and processing of products, customers and orders.

[tests-products] Tests have been covered for products to verify that:
1) function get_absolute_file_path
   * returns a non null value
   * returned value is as expected
2) function create_products_raw_df
   * raises pyspark.errors.exceptions.captured.AnalysisException when file path doesn't exists
   * returns DataFrame for an existing path
   * DataFrame field name and types are as expected
   * Values in the DataFrame columns are non null as expected
3) function create_delta_table when read back as a DataFrame for both raw and enriched tables
   * has same field and field types as the original DataFrame
   * DataFrame values matches that of the original DataFrame
4) function create_products_enriched_df
   * raises pyspark.errors.exceptions.captured.AnalysisException when delta table doesn't exists
   * returns DataFrame for an existing path
   * DataFrame field name and types are as expected for enriched DataFrame
   * Values in the DataFrame columns are non null as expected

[tests-customers] Tests have been covered for customers to verify that:
1) function create_customers_raw_df
   * raises py4j.protocol.Py4JJavaError when file path doesn't exists
   * returns DataFrame for an existing path
   * DataFrame field name and types are as expected
   * Values in the DataFrame columns are non null as expected
3) function create_delta_table when read back as a DataFrame for both raw and enriched tables
   * has same field and field types as the original DataFrame
   * DataFrame values matches that of the original DataFrame
4) function create_customers_enriched_df
   * raises pyspark.errors.exceptions.captured.AnalysisException when delta table doesn't exists
   * returns DataFrame for an existing path
   * DataFrame field name and types are as expected for enriched DataFrame
   * Values in the DataFrame columns are non null except for customerName that has 8 rows with empty(null) customerName, as expected

[tests-orders] Tests have been covered for orders to verify that:
1) function create_orders_raw_df
   * raises pyspark.errors.exceptions.captured.AnalysisException when file path doesn't exists
   * returns DataFrame for an existing table path
   * DataFrame field name and types are as expected
   * Values in the DataFrame columns are non null as expected
3) function create_delta_table when read back as a DataFrame for raw, enriched and aggregated tables
   * has same field and field types as the original DataFrame
   * DataFrame values matches that of the original DataFrame
4) function create_orders_enriched_df
   * raises pyspark.errors.exceptions.captured.AnalysisException when delta table doesn't exists
   * returns DataFrame for an existing table path
   * DataFrame field name and types are as expected for enriched DataFrame
   * Values in the DataFrame columns are non null except for category, subCategory and customerName, as expected
5) function create_orders_aggregated_df
   * raises pyspark.errors.exceptions.captured.AnalysisException when delta table doesn't exists
   * returns DataFrame for an existing table path
   * DataFrame field name and types are as expected for enriched DataFrame
   * Values in the DataFrame columns are non null except for category and subCategory, as expected
   * Year value is extracted from date as expected

#### requirements.txt
Most of the requirements were available by default on the Databricks. Following maven library had to be installed to read xlsx files:
com.crealytics:spark-excel_2.12:3.5.0_0.20.3

