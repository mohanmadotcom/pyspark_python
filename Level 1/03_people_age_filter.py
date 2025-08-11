from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data: (name, age)
data = [("Alice", 25), ("Bob", 32), ("Charlie", 29), ("David", 40)]
df = spark.createDataFrame(data, ["name", "age"])

# Show all data
print("Original Data:")
df.show()

# Filter people over age 30
filtered_df = df.filter(col("age") > 30)

# Show the result
print("People over age 30:")
filtered_df.show()
