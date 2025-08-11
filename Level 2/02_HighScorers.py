from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HighScorers").getOrCreate()

# Sample data
data = [
    (1, "Alice", 88),
    (2, "Bob", 70),
    (3, "Charlie", 92)
]
columns = ["student_id", "name", "score"]
students_df = spark.createDataFrame(data, schema=columns)

# Filter and select
high_scorers = students_df.filter(students_df.score > 75).select("student_id", "name")
high_scorers.show()
