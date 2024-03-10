# Databricks notebook source
import unittest

from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql.types import StructField, StringType, DoubleType, StructType

# COMMAND ----------

# MAGIC %run ../scripts/E-Commerce-Sales-Scripts

# COMMAND ----------

class TestProductsIngestion(unittest.TestCase):

    def test_absolute_file_path(self):
        file_path = "/data/Product.csv"
        absolute_file_path = get_absolute_file_path(file_path)
        expected_absolute_file_path = "file:/Workspace/Repos/prajyotnaik3pn+2024db1@gmail.com/E-Commerce-Sales-Data-Processing-with-Databricks/data/Product.csv"
        self.assertTrue(absolute_file_path)
        self.assertEqual(absolute_file_path, expected_absolute_file_path)
    
    def test_nonexistent_file(self):
        file_path = "/data/Product.csv"
        with self.assertRaises(AnalysisException):
            create_products_raw_df(file_path)

    def test_create_products(self):
        file_path = get_absolute_file_path("/data/Product.csv")
        expected_columns_raw = [('productID', 'string'), ('category', 'string'), ('subCategory', 'string'),  ('productName', 'string'), ('state', 'string'), ('pricePerProduct', 'string')]
        table_raw = "ecommerce_sales_test.products_raw"
        expected_columns_enriched = [('productID', 'string'), ('category', 'string'), ('subCategory', 'string'),  ('productName', 'string'), ('state', 'string'), ('pricePerProduct', 'double')]
        table_enriched = "ecommerce_sales_test.products_enriched"

        df = create_products_raw_df(file_path)

        self.assertTrue(df)
        self.assertEqual(df.dtypes, expected_columns_raw)
        self.assertEqual(df.filter(df["productID"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["category"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["subCategory"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["productName"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["state"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["pricePerProduct"].isNull()).count(), 0)

        create_delta_table(table_raw, df)

        df2 = spark.table(table_raw)

        self.assertEqual(df.dtypes, df2.dtypes)
        self.assertEqual(df.exceptAll(df2).rdd.isEmpty(), True)
        self.assertEqual(df2.exceptAll(df).rdd.isEmpty(), True)

        with self.assertRaises(AnalysisException):
            df3 = create_products_enriched_df("ecommerce_sales_test.products_raw_temp")

        df3 = create_products_enriched_df(table_raw)

        self.assertTrue(df3)
        self.assertEqual(df3.dtypes, expected_columns_enriched)
        self.assertEqual(df3.filter(df3["productID"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["category"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["subCategory"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["productName"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["state"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["pricePerProduct"].isNull()).count(), 0)

        create_delta_table(table_enriched, df3)
        df4 = spark.table(table_enriched)

        self.assertEqual(df3.dtypes, df4.dtypes)
        self.assertEqual(df3.exceptAll(df4).rdd.isEmpty(), True)
        self.assertEqual(df4.exceptAll(df3).rdd.isEmpty(), True)


r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), 'Test failed; see logs above'
