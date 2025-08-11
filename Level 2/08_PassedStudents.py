# Program: PassedStudents (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PassedStudents").getOrCreate()

data = [(1, "Alice", 39), (2, "Bob", 85), (3, "Charlie", 40)]
columns = ["student_id", "name", "score"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("score >= 40").select("student_id", "name")
result.show()
