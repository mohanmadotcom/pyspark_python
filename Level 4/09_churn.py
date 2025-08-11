# pyspark_task9.py
from pyspark_setup import orders, customers
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import max as _max
from datetime import date, timedelta
from pyspark.sql.functions import col

today = F.lit("2025-08-10").cast("date")  # fixed "today" for deterministic results
last_order = (orders.groupBy("customer_id").agg(_max("order_date").alias("last_order_date")))
churned = last_order.filter(F.datediff(today, col("last_order_date")) > 90).join(customers, "customer_id")
churned.show()
