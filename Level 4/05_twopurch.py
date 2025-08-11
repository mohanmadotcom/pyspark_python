# pyspark_task5.py
from pyspark_setup import order_items, customers
import pyspark.sql.functions as F
from pyspark.sql.functions import col

cat_counts = (order_items.filter(~col("is_returned"))
              .groupBy("customer_id")
              .agg(F.countDistinct("category").alias("distinct_cats")))
res = cat_counts.filter(col("distinct_cats") >= 2).join(customers, "customer_id")
res.show()
