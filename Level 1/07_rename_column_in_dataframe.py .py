from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 32)]
df = spark.createDataFrame(data, ["name", "age"])

# Rename column 'name' to 'full_name'
df_renamed = df.withColumnRenamed("name", "full_name")

# Show result
print("DataFrame with renamed column:")
df_renamed.show()
