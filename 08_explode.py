# pyspark_task8.py
from pyspark.sql import Row
from pyspark_setup import spark
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# create a table with items as array of structs (demonstrates explode)
orders_with_items = spark.createDataFrame([
    (5001, 1, [Row(product_id=10, qty=1, unit_price=20.0), Row(product_id=20, qty=2, unit_price=8.0)]),
    (5002, 2, [Row(product_id=11, qty=1, unit_price=40.0)]),
], ["order_id", "customer_id", "items"])

exploded = orders_with_items.withColumn("item", F.explode("items")).select(
    "order_id", "customer_id",
    col("item.product_id").alias("product_id"),
    col("item.qty").alias("qty"),
    col("item.unit_price").alias("unit_price"))
exploded = exploded.withColumn("price", col("qty") * col("unit_price"))
# aggregate total per product
agg = exploded.groupBy("product_id").agg(F.sum("price").alias("total_revenue"))
agg.show()
