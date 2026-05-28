from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder \
    .appName("LAB4_Cau3") \
    .getOrCreate()

orders = spark.read.csv(
    "Orders.csv",
    header=True,
    sep=";",
    inferSchema=True
)

customers = spark.read.csv(
    "Customer_List.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# preprocessing
orders = orders.dropna(subset=["Customer_Trx_ID"])
customers = customers.dropna(subset=["Customer_Trx_ID"])

# join 2 bảng orders và customers theo Customer_Trx_ID
df = orders.join(customers, on="Customer_Trx_ID", how="inner")

result = df.groupBy("Customer_Country") \
    .agg(count("Order_ID").alias("Total_Orders")) \
    .orderBy(col("Total_Orders").desc())

result.show(truncate=False)

result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau3")

spark.stop()