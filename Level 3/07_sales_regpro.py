# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("Q7_pivot").getOrCreate()
data = [("North","P1",100.0),("North","P2",50.0),("South","P1",70.0)]
df = spark.createDataFrame(data, ["region","product","amount"])
pivoted = df.groupBy("region").pivot("product").agg(_sum("amount"))
pivoted.show()
spark.stop()
