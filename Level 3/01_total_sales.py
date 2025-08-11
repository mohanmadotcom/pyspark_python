# PySpark (run in PySpark / spark-submit or notebook)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("Q1_total_sales_per_product").getOrCreate()

data = [
    (1, "P1", 2, 10.0),
    (2, "P2", 1, 20.0),
    (3, "P1", 3, 10.0),
    (4, "P3", 5, 5.0)
]
df = spark.createDataFrame(data, ["order_id","product_id","quantity","price"])
result = df.withColumn("sale", col("quantity") * col("price")) \
           .groupBy("product_id") \
           .agg(_sum("sale").alias("total_sales"))
result.show()
spark.stop()
