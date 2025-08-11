# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Q4_join").getOrCreate()
orders = spark.createDataFrame([(1,"C1",100.0),(2,"C2",50.0)], ["order_id","customer_id","amount"])
customers = spark.createDataFrame([("C1","Alice"),("C3","Charlie")], ["customer_id","name"])
joined = orders.join(customers, on="customer_id", how="inner") \
               .select("order_id","name","amount")
joined.show()
spark.stop()
