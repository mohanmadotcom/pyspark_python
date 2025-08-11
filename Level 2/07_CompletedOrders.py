# Program: CompletedOrders (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CompletedOrders").getOrCreate()

data = [(1, "Alice", "Pending"), (2, "Bob", "Completed"), (3, "Charlie", "Completed")]
columns = ["order_id", "customer", "status"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("status = 'Completed'").select("order_id", "customer")
result.show()
