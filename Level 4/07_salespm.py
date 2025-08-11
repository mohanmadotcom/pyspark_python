# pyspark_task7.py
from pyspark_setup import order_items
from pyspark.sql.functions import col
import pyspark.sql.functions as F

pivoted = (order_items.filter(~col("is_returned"))
           .withColumn("ym", F.date_format(col("order_date"), "yyyy-MM"))
           .groupBy("ym")
           .pivot("category")
           .agg(F.sum("price")))
pivoted.show(truncate=False)
