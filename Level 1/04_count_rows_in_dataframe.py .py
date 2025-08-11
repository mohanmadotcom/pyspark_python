from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 32), ("Charlie", 29), ("David", 40)]
df = spark.createDataFrame(data, ["name", "age"])

# Show the data
print("Data:")
df.show()

# Count number of rows
row_count = df.count()

print(f"\nNumber of rows: {row_count}")
