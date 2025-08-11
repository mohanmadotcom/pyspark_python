# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, StructType

spark = SparkSession.builder.appName("Q8_explode").getOrCreate()
data = [
    (1, [{"item_id":"I1","qty":2}, {"item_id":"I2","qty":1}]),
    (2, [{"item_id":"I1","qty":3}])
]
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType()),
        StructField("qty", IntegerType())
    ])))
])
df = spark.createDataFrame(data, schema=schema)
expl = df.select(explode(col("items")).alias("item"))
res = expl.groupBy(col("item.item_id").alias("item_id")) \
          .agg(_sum(col("item.qty")).alias("total_qty"))
res.show()
spark.stop()
