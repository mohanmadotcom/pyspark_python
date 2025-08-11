from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("FilterActiveUsers").getOrCreate()

# Sample data (if needed for testing)
data = [
    (1, "Alice", True),
    (2, "Bob", False),
    (3, "Charlie", True)
]

# Define schema and create DataFrame
columns = ["user_id", "name", "is_active"]
users_df = spark.createDataFrame(data, schema=columns)

# Step 1 & 2: Filter active users and select user_id and name
active_users_df = users_df.filter(users_df.is_active == True).select("user_id", "name")

# Step 3: Show the result
active_users_df.show()
