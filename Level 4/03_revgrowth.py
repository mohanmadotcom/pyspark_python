# pyspark_task3.py
from pyspark_setup import order_items
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

monthly = (order_items.filter(~col("is_returned"))
           .withColumn("year_month", F.date_format(col("order_date"), "yyyy-MM"))
           .groupBy("year_month")
           .agg(F.sum("price").alias("monthly_revenue"))
           .orderBy("year_month"))

w = Window.orderBy("year_month")
monthly = monthly.withColumn("prev_rev", lag("monthly_revenue").over(w))
monthly = monthly.withColumn("pct_change", (col("monthly_revenue") - col("prev_rev")) / col("prev_rev") * 100)
monthly.show(truncate=False)
