from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data with a null value
data = [("Alice", 25), ("Bob", None), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])

# Show original data
print("Original DataFrame:")
df.show()

# Count nulls in 'age' column
print("Count of nulls in each column:")
df.select([col(c).isNull().alias(c + "_is_null") for c in df.columns]).show()
