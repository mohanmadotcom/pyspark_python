# Program: AdultsOnly (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AdultsOnly").getOrCreate()

data = [(1, "Alice", 17), (2, "Bob", 25), (3, "Charlie", 18)]
columns = ["user_id", "name", "age"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("age >= 18").select("user_id", "name")
result.show()
