# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg as _avg
from pyspark.sql.window import Window
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Q3_moving_avg").getOrCreate()
data = [
    ("2025-08-01", 100.0),
    ("2025-08-02", 200.0),
    ("2025-08-03", 50.0),
    ("2025-08-04", 150.0)
]
df = spark.createDataFrame(data, ["dt","amount"]).withColumn("dt", col("dt").cast("date"))
w = Window.orderBy("dt").rowsBetween(-2, 0)
res = df.withColumn("3day_avg", _avg("amount").over(w))
res.show()
spark.stop()
