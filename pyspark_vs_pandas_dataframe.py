from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()
