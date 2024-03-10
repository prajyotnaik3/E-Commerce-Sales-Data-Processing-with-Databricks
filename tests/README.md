tests directory contains test cases written testing ingestion and processing of products, customers and orders.

Tests have been covered for products to verify that:
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

Tests have been covered for customers to verify that:
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

Tests have been covered for orders to verify that:
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
