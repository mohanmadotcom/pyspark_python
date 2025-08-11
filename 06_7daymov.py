from pyspark_setup import order_items
import pyspark.sql.functions as F
from pyspark.sql.window import Window

daily = (order_items.filter(~F.col("is_returned"))
         .groupBy("order_date")
         .agg(F.sum("price").alias("daily_sales")))

# Option 1: use DATE ordering directly
w = Window.orderBy("order_date").rowsBetween(-6, 0)
daily = daily.withColumn("rolling_7d_avg", F.avg("daily_sales").over(w))

# Option 2: convert to int days for ordering (if you want explicit numeric sort)
# w = Window.orderBy(F.unix_date("order_date")).rowsBetween(-6, 0)

daily.orderBy("order_date").show(truncate=False)
