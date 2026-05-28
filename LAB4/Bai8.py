from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, avg, when

spark = SparkSession.builder \
    .appName("LAB4_Cau8") \
    .getOrCreate()

# read data
orders = spark.read.csv(
    "Orders.csv",
    header=True,
    sep=";",
    inferSchema=True
)

order_items = spark.read.csv(
    "Order_Items.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# cast datetime
orders = orders.withColumn(
    "Order_Delivered_Carrier_Date",
    to_timestamp("Order_Delivered_Carrier_Date")
)

order_items = order_items.withColumn(
    "Shipping_Limit_Date",
    to_timestamp("Shipping_Limit_Date")
)

# join
df = order_items.join(orders, "Order_ID", "inner")

# tính chênh lệch 
df = df.withColumn(
    "Delivery_Delay_Days",
    datediff(
        col("Order_Delivered_Carrier_Date"),
        col("Shipping_Limit_Date")
    )
)

df = df.withColumn(
    "Delivery_Status",
    when(col("Delivery_Delay_Days") < 0, "EARLY")
    .when(col("Delivery_Delay_Days") == 0, "ON_TIME")
    .otherwise("LATE")
)

# result detail
result = df.select(
    "Order_ID",
    "Product_ID",
    "Shipping_Limit_Date",
    "Order_Delivered_Carrier_Date",
    "Delivery_Delay_Days",
    "Delivery_Status"
)

result.show(10, truncate=False)

# stats
stats = df.select(
    avg("Delivery_Delay_Days").alias("Avg_Delay_Days")
)

stats.show()

# save
result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau8")

spark.stop()