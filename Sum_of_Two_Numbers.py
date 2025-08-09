from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Take user input
num1 = int(input("Enter first number: "))
num2 = int(input("Enter second number: "))

# Create DataFrame with input numbers
data = [(num1, num2)]
df = spark.createDataFrame(data, ["num1", "num2"])

# Show the input numbers
df.show()

# Add a column for the sum
df_with_sum = df.withColumn("sum", col("num1") + col("num2"))

# Show the result with the sum
df_with_sum.show()
