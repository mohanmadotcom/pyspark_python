# pyspark_task2.py
from pyspark_setup import order_items, spark
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank
import pyspark.sql.functions as F

rev = (order_items.filter(~col("is_returned"))
       .groupBy("customer_id", "product_id", "product_name")
       .agg(F.sum("price").alias("revenue")))
w = Window.partitionBy("customer_id").orderBy(F.desc("revenue"))
ranked = rev.withColumn("rnk", rank().over(w)).filter(col("rnk") <= 3).orderBy("customer_id", "rnk")
ranked.show(truncate=False)
