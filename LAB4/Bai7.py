from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("LAB4_Cau7") \
    .getOrCreate()

order_items = spark.read.csv("Order_Items.csv", header=True, sep=";", inferSchema=True)
reviews = spark.read.csv("Order_Reviews.csv", header=True, sep=";", inferSchema=True)

# cast
reviews = reviews.withColumn("Review_Score", F.col("Review_Score").cast("int"))

# sản phẩm bán ra
product_sales = order_items.groupBy("Product_ID").agg(
    F.count("Order_Item_ID").alias("Total_Sold")
)

# review theo order trước (tránh duplicate logic sai)
order_reviews = reviews.select("Order_ID", "Review_Score")

# join đúng
df = order_items.join(order_reviews, "Order_ID", "left")

# rating theo product
product_rating = df.groupBy("Product_ID").agg(
    F.avg("Review_Score").alias("Avg_Review_Score"),
    F.count("Review_Score").alias("Total_Reviews")
)

# combine
final_df = product_sales.join(product_rating, "Product_ID", "left") \
    .orderBy(F.desc("Total_Sold"))

# top product
print("TOP 10 PRODUCT:")
final_df.show(10, truncate=False)

# save
final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau7")

spark.stop()