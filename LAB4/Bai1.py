from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LAB4_Cau1") \
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

order_items = spark.read.csv(
    "Order_Items.csv",
    header=True,
    sep=";",
    inferSchema=True
)

products = spark.read.csv(
    "Products.csv",
    header=True,
    sep=";",
    inferSchema=True
)

reviews = spark.read.csv(
    "Order_Reviews.csv",
    header=True,
    sep=";",
    inferSchema=True
)

# show 5 dòng đầu tiên của mỗi dataframe để kiểm tra dữ liệu đã đọc đúng chưa
print("===== ORDERS =====")
orders.show(5, truncate=False)

print("===== CUSTOMERS =====")
customers.show(5, truncate=False)

print("===== ORDER ITEMS =====")
order_items.show(5, truncate=False)

print("===== PRODUCTS =====")
products.show(5, truncate=False)

print("===== REVIEWS =====")
reviews.show(5, truncate=False)

# print schema 
orders.printSchema()
customers.printSchema()
order_items.printSchema()
products.printSchema()
reviews.printSchema()

spark.stop()
