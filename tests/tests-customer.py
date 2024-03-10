# Databricks notebook source
import unittest
from py4j.protocol import Py4JJavaError
from pyspark.sql.types import StructField, StringType, DoubleType, StructType

# COMMAND ----------

# MAGIC %run ../scripts/E-Commerce-Sales-Scripts

# COMMAND ----------

class TestCustomerIngestion(unittest.TestCase):

    def test_nonexistent_file(self):
        file_path = "/data/Customer.xlsx"
        with self.assertRaises(Py4JJavaError):
            create_customers_raw_df(file_path)

    def test_create_customers(self):
        file_path = get_absolute_file_path("/data/Customer.xlsx")
        expected_columns_raw = [('customerID', 'string'), ('customerName', 'string'), ('email', 'string'),  ('phone', 'string'), ('address', 'string'), ('segment', 'string'), ('country', 'string'), ('city', 'string'), ('state', 'string'), ('postalCode', 'string'), ('region', 'string')]
        table_raw = "ecommerce_sales_test.customers_raw"
        expected_columns_enriched = [('customerID', 'string'), ('customerName', 'string'), ('email', 'string'),  ('phone', 'string'), ('address', 'string'), ('segment', 'string'), ('country', 'string'), ('city', 'string'), ('state', 'string'), ('postalCode', 'string'), ('region', 'string')]
        table_enriched = "ecommerce_sales_test.customers_enriched"
        
        df = create_customers_raw_df(file_path)
        self.assertTrue(df)
        self.assertEqual(df.dtypes, expected_columns_raw)
        self.assertEqual(df.filter(df["customerID"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["customerName"].isNull()).count(), 8)
        self.assertEqual(df.filter(df["email"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["phone"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["address"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["segment"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["country"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["city"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["state"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["postalCode"].isNull()).count(), 0)
        self.assertEqual(df.filter(df["region"].isNull()).count(), 0)

        create_delta_table(table_raw, df)
        df2 = spark.table(table_raw)

        self.assertTrue(df2)
        self.assertEqual(df.dtypes, df2.dtypes)
        self.assertEqual(df2.exceptAll(df).rdd.isEmpty(), True)
        self.assertEqual(df.exceptAll(df2).rdd.isEmpty(), True)
    
        df3 = create_customers_enriched_df(table_raw)

        self.assertTrue(df3)
        self.assertEqual(df3.dtypes, expected_columns_enriched)
        self.assertEqual(df3.filter(df3["customerID"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["customerName"].isNull()).count(), 8)
        self.assertEqual(df3.filter(df3["email"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["phone"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["address"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["segment"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["country"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["city"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["state"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["postalCode"].isNull()).count(), 0)
        self.assertEqual(df3.filter(df3["region"].isNull()).count(), 0)
        
        create_delta_table(table_enriched, df3)
        df4 = spark.table(table_enriched)
        self.assertEqual(df3.dtypes, df4.dtypes)
        self.assertEqual(df3.exceptAll(df4).rdd.isEmpty(), True)
        self.assertEqual(df4.exceptAll(df3).rdd.isEmpty(), True)

r = unittest.main(argv=[''], verbosity=2, exit=False)
assert r.result.wasSuccessful(), 'Test failed; see logs above'
