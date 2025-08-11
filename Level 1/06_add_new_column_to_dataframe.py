from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 32), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])

# Add a new column 'country' with the same value for all rows
df_with_country = df.withColumn("country", lit("India"))

# Show result
print("DataFrame with new column:")
df_with_country.show()
