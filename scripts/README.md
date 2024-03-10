scripts contains the functions used to process the e-commerce sales data using Databricks

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
