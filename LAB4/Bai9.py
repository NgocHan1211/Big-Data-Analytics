from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, avg, sum as _sum, when, min, max

spark = SparkSession.builder \
    .appName("LAB4_Cau9") \
    .getOrCreate()

# đọc dữ liệu
orders = spark.read.csv("Orders.csv", header=True, sep=";", inferSchema=True)
order_items = spark.read.csv("Order_Items.csv", header=True, sep=";", inferSchema=True)

# cast
order_items = order_items.withColumn("Price", col("Price").cast("double")) \
                         .withColumn("Freight_Value", col("Freight_Value").cast("double"))

# order value
order_items = order_items.withColumn(
    "Order_Value",
    col("Price") + col("Freight_Value")
)

# tổng value mỗi order
order_value = order_items.groupBy("Order_ID").agg(
    _sum("Order_Value").alias("Order_Value")
)

# join orders
df = orders.join(order_value, "Order_ID", "inner")

# parse time
df = df.withColumn("Order_Date", col("Order_Purchase_Timestamp").cast("timestamp"))

# customer features
customer_segment = df.groupBy("Customer_Trx_ID").agg(
    count("Order_ID").alias("Total_Orders"),
    avg("Order_Value").alias("Avg_Order_Value"),
    min("Order_Date").alias("First_Order"),
    max("Order_Date").alias("Last_Order")
)

# frequency = khoảng thời gian hoạt động (proxy)
customer_segment = customer_segment.withColumn(
    "Active_Days",
    F.datediff(col("Last_Order"), col("First_Order"))
)

# segment logic
customer_segment = customer_segment.withColumn(
    "Segment",
    when(
        (col("Total_Orders") >= 10) &
        (col("Avg_Order_Value") >= 100) &
        (col("Active_Days") >= 30),
        "VIP"
    )
    .when(
        (col("Total_Orders") >= 5) &
        (col("Avg_Order_Value") >= 50),
        "Loyal"
    )
    .otherwise("Normal")
)

customer_segment.show(truncate=False)

customer_segment.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau9")

spark.stop()