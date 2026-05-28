from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# tạo Spark
spark = SparkSession.builder \
    .appName("LAB4_Cau10") \
    .getOrCreate()

# đọc dữ liệu
order_items = spark.read.csv(
    "Order_Items.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# cast dữ liệu
order_items = order_items.withColumn("Price", F.col("Price").cast("double")) \
                         .withColumn("Freight_Value", F.col("Freight_Value").cast("double"))

# tính doanh thu
order_items = order_items.withColumn(
    "Revenue",
    F.col("Price") + F.col("Freight_Value")
)

# group theo seller
seller_stats = order_items.groupBy("Seller_ID").agg(
    F.sum("Revenue").alias("Total_Revenue"),
    F.countDistinct("Order_ID").alias("Total_Orders")
)

# window ranking
w_revenue = Window.orderBy(F.desc("Total_Revenue"))
w_orders  = Window.orderBy(F.desc("Total_Orders"))

# xếp hạng
seller_ranked = seller_stats \
    .withColumn("Revenue_Rank", F.dense_rank().over(w_revenue)) \
    .withColumn("Orders_Rank", F.dense_rank().over(w_orders)) \
    .withColumn("Overall_Rank_Score",
                F.col("Revenue_Rank") + F.col("Orders_Rank")) \
    .orderBy("Revenue_Rank")

# hiển thị kết quả
print("TOP 20 SELLER THEO DOANH THU:")
seller_ranked.show(20, truncate=False)

# lưu file
seller_ranked.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau10_xep_hang_seller")

spark.stop()