from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, year, month, to_timestamp

spark = SparkSession.builder \
    .appName("LAB4_Cau4") \
    .getOrCreate()

orders = spark.read.csv(
    "Orders.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# preprocessing
orders = orders.dropna(subset=["Order_Purchase_Timestamp"])

orders = orders.withColumn(
    "Order_Purchase_Timestamp",
    to_timestamp("Order_Purchase_Timestamp")
)

# extract year + month từ timestamp
orders = orders.withColumn("year", year("Order_Purchase_Timestamp")) \
               .withColumn("month", month("Order_Purchase_Timestamp"))

# group by year + month, đếm số lượng order
result = orders.groupBy("year", "month") \
    .agg(count("Order_ID").alias("Total_Orders")) \
    .orderBy(col("year").asc(), col("month").desc())

result.show(truncate=False)

result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau4")

spark.stop()