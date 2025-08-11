# Program: PremiumCustomers (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PremiumCustomers").getOrCreate()

data = [(1, "Alice", "Standard"), (2, "Bob", "Premium"), (3, "Charlie", "Premium")]
columns = ["customer_id", "name", "membership"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("membership = 'Premium'").select("customer_id", "name")
result.show()
