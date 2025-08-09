from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data with duplicates
data = [("Alice", 25), ("Bob", 32), ("Alice", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Show original data
print("Original DataFrame:")
df.show()

# Remove duplicate rows
df_no_duplicates = df.dropDuplicates()

# Show result
print("DataFrame after removing duplicates:")
df_no_duplicates.show()
