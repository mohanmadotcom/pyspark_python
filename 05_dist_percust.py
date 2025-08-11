# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder.appName("Q5_distinct_products").getOrCreate()
data = [(1,"C1","P1"),(2,"C1","P2"),(3,"C1","P1"),(4,"C2","P3")]
df = spark.createDataFrame(data, ["order_id","customer_id","product_id"])
res = df.groupBy("customer_id").agg(countDistinct("product_id").alias("distinct_products"))
res.show()
spark.stop()
