# pyspark_task10.py
from pyspark_setup import order_items
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col

pd = (order_items.filter(~col("is_returned"))
      .groupBy("product_id", "order_date")
      .agg(F.sum("price").alias("daily_rev")))

# compute product-level mean and std
stats = pd.groupBy("product_id").agg(F.mean("daily_rev").alias("mu"), F.stddev("daily_rev").alias("sigma"))
joined = pd.join(stats, "product_id")
joined = joined.withColumn("z", (col("daily_rev") - col("mu")) / col("sigma"))
anomalies = joined.filter((col("z") > 3) | (col("z") < -3))
anomalies.show(truncate=False)
