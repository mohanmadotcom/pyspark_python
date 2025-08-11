from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 32), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])

# Sort the DataFrame by age in ascending order
sorted_df = df.orderBy("age")

# Show result
print("Data sorted by age:")
sorted_df.show()
