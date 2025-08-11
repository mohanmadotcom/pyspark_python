# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import min as _min, col

spark = SparkSession.builder.appName("Q6_repeat_flag").getOrCreate()
data = [(1,"C1","2025-08-01"),(2,"C1","2025-08-05"),(3,"C2","2025-08-03")]
df = spark.createDataFrame(data, ["order_id","customer_id","order_date"]) \
          .withColumn("order_date", col("order_date").cast("date"))
w = Window.partitionBy("customer_id")
first = df.withColumn("first_order_date", _min("order_date").over(w))
res = first.withColumn("is_repeat", col("order_date") > col("first_order_date")) \
           .select("order_id","customer_id","order_date","is_repeat")
res.orderBy("order_id").show()
spark.stop()
