# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("Q9_dedup_latest").getOrCreate()
data = [
    (1, "C1", "2025-08-01 10:00:00"),
    (1, "C1-updated", "2025-08-01 12:00:00"),
    (2, "C2", "2025-08-02 09:00:00")
]
df = spark.createDataFrame(data, ["order_id","info","updated_at"]) \
          .withColumn("updated_at", col("updated_at").cast("timestamp"))
w = Window.partitionBy("order_id").orderBy(desc("updated_at"))
dedup = df.withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")
dedup.show()
spark.stop()
