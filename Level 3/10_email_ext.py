# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count

spark = SparkSession.builder.appName("Q10_email_domain").getOrCreate()
data = [(1,"alice@gmail.com"),(2,"bob@yahoo.com"),(3,"charlie@gmail.com")]
df = spark.createDataFrame(data, ["user_id","email"])
# regex: capture part after @
domain = regexp_extract(col("email"), r'@([^@]+)$', 1)
res = df.withColumn("domain", domain).groupBy("domain").agg(count("*").alias("user_count"))
res.show()
spark.stop()
