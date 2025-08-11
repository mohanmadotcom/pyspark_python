# pyspark_setup.py (common setup for all PySpark examples)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum as _sum, avg as _avg, countDistinct, rank, month, year, to_date, lag, when, explode, array, size, stddev, mean
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").appName("Level4Examples").getOrCreate()

# Sample tables: customers, products, orders, order_items
customers = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Carol"),
    (4, "Dan"),
], ["customer_id", "name"])

products = spark.createDataFrame([
    (10, "Red T-Shirt", "Apparel"),
    (11, "Blue Jeans", "Apparel"),
    (20, "Coffee Mug", "Home"),
    (21, "Tea Kettle", "Home"),
], ["product_id", "product_name", "category"])

orders = spark.createDataFrame([
    (1001, 1, "2025-06-01"),
    (1002, 1, "2025-06-03"),
    (1003, 2, "2025-06-10"),
    (1004, 2, "2025-07-01"),
    (1005, 3, "2025-05-15"),
    (1006, 4, "2025-07-10"),
    (1007, 1, "2025-07-11"),
], ["order_id", "customer_id", "order_date"]).withColumn("order_date", to_date(col("order_date")))

order_items = spark.createDataFrame([
    (1001, 10, 2, 20.0, False),
    (1001, 20, 1, 8.0, False),
    (1002, 11, 1, 40.0, True),   # returned
    (1003, 10, 3, 20.0, False),
    (1004, 21, 1, 35.0, False),
    (1005, 20, 2, 8.0, False),
    (1006, 11, 1, 40.0, False),
    (1007, 10, 1, 20.0, False),
], ["order_id", "product_id", "qty", "unit_price", "is_returned"])

# Enrich order_items with product & order info
order_items = order_items.join(products, "product_id", "left").join(orders, "order_id", "left")
order_items = order_items.withColumn("price", col("qty") * col("unit_price"))
order_items.cache()
