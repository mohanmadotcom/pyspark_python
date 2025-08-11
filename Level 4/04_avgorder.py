# pyspark_task4.py
from pyspark_setup import order_items
from pyspark.sql.functions import col
import pyspark.sql.functions as F

order_totals = (order_items.filter(~col("is_returned"))
                .groupBy("order_id")
                .agg(F.sum("price").alias("order_total")))
avg_order_value = order_totals.agg(F.avg("order_total").alias("avg_order_value"))
avg_order_value.show()
