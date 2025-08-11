# Program: ActiveSubscriptions (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ActiveSubscriptions").getOrCreate()

data = [(1, "alice", True), (2, "bob", False), (3, "charlie", True)]
columns = ["user_id", "username", "is_active"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("is_active = True").select("user_id", "username")
result.show()
