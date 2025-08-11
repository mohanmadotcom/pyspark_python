# pyspark_task1.py
from pyspark_setup import order_items, customers, spark
from pyspark.sql.functions import col, sum as _sum

res = (order_items
       .filter(~col("is_returned"))
       .groupBy("customer_id")
       .agg(_sum("price").alias("total_sales"))
       .join(customers, "customer_id", "left")
       .orderBy(col("total_sales").desc()))
res.show(truncate=False)
