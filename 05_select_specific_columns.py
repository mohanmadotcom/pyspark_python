from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Alice", 25, "HR"), ("Bob", 32, "IT"), ("Charlie", 29, "Finance")]
df = spark.createDataFrame(data, ["name", "age", "department"])

# Show full data
print("Full DataFrame:")
df.show()

# Select only 'name' and 'department'
selected_df = df.select("name", "department")

# Show result
print("Selected Columns:")
selected_df.show()
