from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Fecom Analysis") \
    .getOrCreate()

orders = spark.read.csv(
    "/home/ngochan1211/LAB4/Orders.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

customers = spark.read.csv(
    "/home/ngochan1211/LAB4/Customer_List.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

order_items = spark.read.csv(
    "/home/ngochan1211/LAB4/Order_Items.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

# thống kê

total_orders = orders.select("Order_ID").distinct().count()
total_customers = customers.select("Customer_Trx_ID").distinct().count()
total_sellers = order_items.select("Seller_ID").distinct().count()

print("===== THỐNG KÊ =====")
print("Tổng số đơn hàng:", total_orders)
print("Tổng số khách hàng:", total_customers)
print("Tổng số người bán:", total_sellers)

from pyspark.sql import Row

result_df = spark.createDataFrame([
    Row(Metric="Total_Orders", Value=total_orders),
    Row(Metric="Total_Customers", Value=total_customers),
    Row(Metric="Total_Sellers", Value=total_sellers)
])

result_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/home/ngochan1211/LAB4/output/bai2_thongke")

print(" Đã export xong file output!")