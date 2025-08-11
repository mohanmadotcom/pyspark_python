# Program: HighRatings (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HighRatings").getOrCreate()

data = [(1, "Movie A", 4.4), (2, "Movie B", 4.7), (3, "Movie C", 5.0)]
columns = ["movie_id", "title", "rating"]
df = spark.createDataFrame(data, schema=columns)

result = df.filter("rating >= 4.5").select("movie_id", "title")
result.show()
