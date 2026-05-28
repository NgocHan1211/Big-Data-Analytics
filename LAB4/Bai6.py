from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_timestamp, year

spark = SparkSession.builder \
    .appName("LAB4_Cau6") \
    .getOrCreate()

order_items = spark.read.csv(
    "/home/ngochan1211/LAB4/Order_Items.csv",
    header=True,
    sep=";",
    inferSchema=True
)

products = spark.read.csv(
    "/home/ngochan1211/LAB4/Products.csv",
    header=True,
    sep=";",
    inferSchema=True
)

orders = spark.read.csv(
    "/home/ngochan1211/LAB4/Orders.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# preprocessing
order_items = order_items.withColumn("Price", col("Price").cast("double")) \
                         .withColumn("Freight_Value", col("Freight_Value").cast("double"))

orders = orders.withColumn(
    "Order_Purchase_Timestamp",
    to_timestamp("Order_Purchase_Timestamp")
)

# filter orders chỉ lấy năm 2024
orders_2024 = orders.filter(year("Order_Purchase_Timestamp") == 2024)

# join 3 bảng order_items, orders_2024, products
df = order_items.join(orders_2024, "Order_ID", "inner") \
                .join(products, "Product_ID", "inner")

# create Revenue column = Price + Freight_Value
df = df.withColumn(
    "Revenue",
    col("Price") + col("Freight_Value")
)

# group by category, tính tổng doanh thu
result = df.groupBy("Product_Category_Name") \
    .agg(
        _sum("Revenue").alias("Total_Revenue")
    ) \
    .orderBy(col("Total_Revenue").desc())

result.show(truncate=False)

result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau6")

spark.stop()