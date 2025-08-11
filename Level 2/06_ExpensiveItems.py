# Program: ExpensiveItems (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExpensiveItems").getOrCreate()

data = [(1, "Chair", 75), (2, "Table", 150), (3, "Lamp", 120)]
columns = ["item_id", "item_name", "price"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("price > 100").select("item_id", "item_name")
result.show()
