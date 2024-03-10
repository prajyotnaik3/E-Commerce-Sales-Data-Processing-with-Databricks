# Databricks notebook source
import unittest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql.types import StructField, StringType, DoubleType, StructType, IntegerType

# COMMAND ----------

# MAGIC %run ../scripts/E-Commerce-Sales-Scripts

# COMMAND ----------

class TestReadJsonFile(unittest.TestCase):
    schema = StructType([
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

    def test_nonexistent_file(self):
        file_path = "/data/Order.json"
        with self.assertRaises(AnalysisException):
            create_products_raw_df(file_path)
    
    def test_clean_price(self):
        self.assertEqual(clean_price("100"), "100")
        self.assertEqual(clean_price("100%"), "100")

    def test_create_orders(self):
        file_path = get_absolute_file_path("/data/Order.json")
        expected_columns_raw = [('rowID', 'bigint'), ('orderID', 'string'), ('orderDate', 'string'),  ('shipDate', 'string'), ('shipMode', 'string'), ('customerID', 'string'), ('productID', 'string'), ('quantity', 'bigint'), ('price', 'string'), ('discount', 'double'), ('profit', 'double')]
        table_raw = "ecommerce_sales_test.orders_raw"
        expected_columns_enriched = [('rowID', 'bigint'), ('orderID', 'string'), ('orderDate', 'date'),  ('shipDate', 'date'), ('shipMode', 'string'), ('customerID', 'string'), ('productID', 'string'), ('quantity', 'bigint'), ('price', 'double'), ('discount', 'double'), ('profit', 'double'), ('category', 'string'), ('subCategory', 'string'), ('customerName', 'string'), ('country', 'string')]
        table_enriched = "ecommerce_sales_test.orders_enriched"
        expected_columns_aggregated = [('customerID', 'string'), ('profit', 'double'), ('category', 'string'), ('subCategory', 'string'), ('year', 'int')]
        table_aggregated = "ecommerce_sales_test.orders_aggregated"

        df = create_orders_raw_df(file_path)

        self.assertTrue(df)
        df_dtypes = {dtype[0]: dtype[1] for dtype in df.dtypes}
        expected_columns_raw_dtypes = {dtype[0]: dtype[1] for dtype in expected_columns_raw}
        self.assertEqual(df_dtypes, expected_columns_raw_dtypes)
        self.assertEqual(df.filter(df["rowID"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["orderID"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["orderDate"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["shipDate"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["shipMode"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["customerID"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["productID"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["quantity"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["price"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["discount"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["profit"].isNull()).count(), 0)
        
        create_delta_table(table_raw, df)
        df2 = spark.table(table_raw)

        self.assertTrue(df2)
        df_dtypes = {dtype[0]: dtype[1] for dtype in df.dtypes}
        df2_dtypes = {dtype[0]: dtype[1] for dtype in df2.dtypes}
        self.assertEqual(df_dtypes, df2_dtypes)
        self.assertEqual(df.exceptAll(df2).rdd.isEmpty(), True)
        self.assertEqual(df2.exceptAll(df).rdd.isEmpty(), True)
        
        df3 = create_orders_enriched_df("ecommerce_sales_test.products_raw", "ecommerce_sales_test.customers_raw", table_raw)

        self.assertTrue(df3)
        df3_dtypes = {dtype[0]: dtype[1] for dtype in df3.dtypes}
        expected_columns_enriched_dtypes = {dtype[0]: dtype[1] for dtype in expected_columns_enriched}
        self.assertEqual(df3_dtypes, expected_columns_enriched_dtypes)
        self.assertEqual(df3.filter(df3["rowID"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["orderID"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["orderDate"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["shipDate"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["shipMode"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["customerID"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["productID"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["quantity"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["price"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["discount"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["profit"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["category"].isNull()).count(), 204)
        self.assertEqual(df3.filter(df3["subCategory"].isNull()).count(), 204)
        self.assertEqual(df3.filter(df3["customerName"].isNull()).count(), 137)
        self.assertEqual(df3.filter(df3["country"].isNull()).count(), 0)
        
        create_delta_table(table_enriched, df3)
        df4 = spark.table(table_enriched)
        self.assertEqual(df3.dtypes, df4.dtypes)
        self.assertEqual(df3.exceptAll(df4).rdd.isEmpty(), True)
        self.assertEqual(df4.exceptAll(df3).rdd.isEmpty(), True)

        df5 = create_orders_aggregated_df(table_enriched)
        
        self.assertTrue(df5)
        df5_dtypes = {dtype[0]: dtype[1] for dtype in df5.dtypes}
        expected_columns_aggregated_dtypes = {dtype[0]: dtype[1] for dtype in expected_columns_aggregated}
        self.assertEqual(df5_dtypes, expected_columns_aggregated_dtypes)
        self.assertEqual(df5.filter(df5["customerID"].isNull()).count(), 0)
        self.assertEqual(df5.filter(df5["profit"].isNull()).count(), 0)
        self.assertEqual(df5.filter(df5["category"].isNull()).count(), 192)
        self.assertEqual(df5.filter(df5["subCategory"].isNull()).count(), 192)
        self.assertEqual(df5.filter(df5["year"].isNull()).count(), 0)

        create_delta_table(table_aggregated, df5)
        df6 = spark.table(table_aggregated)
        self.assertEqual(df5.dtypes, df6.dtypes)
        self.assertEqual(df5.exceptAll(df6).rdd.isEmpty(), True)
        self.assertEqual(df6.exceptAll(df5).rdd.isEmpty(), True)

r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), 'Test failed; see logs above'
