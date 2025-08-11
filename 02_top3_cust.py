# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc

spark = SparkSession.builder.appName("Q2_top3_customers").getOrCreate()
data = [(1,"C1",100.0),(2,"C2",50.0),(3,"C1",30.0),(4,"C3",200.0),(5,"C2",120.0)]
df = spark.createDataFrame(data, ["order_id","customer_id","amount"])
res = df.groupBy("customer_id").agg(_sum("amount").alias("total")) \
        .orderBy(desc("total")).limit(3)
res.show()
spark.stop()
