from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 32), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])

# Sort the DataFrame by age in descending order
sorted_df = df.orderBy(col("age").desc())

# Show result
print("Data sorted by age (descending):")
sorted_df.show()
