from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = SparkSession.builder \
    .appName("LAB4_Cau5") \
    .getOrCreate()

reviews = spark.read.csv(
    "Order_Reviews.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# remove NULL review score
reviews = reviews.dropna(subset=["Review_Score"])

# cast to integer (phòng trường hợp string)
reviews = reviews.withColumn("Review_Score", col("Review_Score").cast("int"))

# giữ chỉ score hợp lệ 1–5
reviews = reviews.filter(
    (col("Review_Score") >= 1) & (col("Review_Score") <= 5)
)

# average review score
avg_score = reviews.select(
    avg("Review_Score").alias("Average_Review_Score")
)

avg_score.show()

# count by score
score_distribution = reviews.groupBy("Review_Score") \
    .agg(count("*").alias("Total_Reviews")) \
    .orderBy(col("Review_Score").asc())

score_distribution.show()

score_distribution.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cau5")

spark.stop()