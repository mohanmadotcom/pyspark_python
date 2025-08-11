# Program: InStockProducts (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InStockProducts").getOrCreate()

data = [(101, "Pen", 0), (102, "Notebook", 20), (103, "Eraser", 5)]
columns = ["product_id", "product_name", "quantity"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("quantity > 0").select("product_id", "product_name")
result.show()
