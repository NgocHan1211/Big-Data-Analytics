
from pyspark import SparkContext
from datetime import datetime

sc = SparkContext.getOrCreate()
ratings1_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_1.txt"
ratings2_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_2.txt"

# timestamp -> year
def get_year(timestamp):

    return datetime.fromtimestamp(
        int(timestamp)
    ).year

# ratings
ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)

ratings = ratings1.union(ratings2)

# Year -> (Rating,1)
ratings_rdd = ratings.map(lambda line: line.split(",")) \
                     .map(
                         lambda x: (
                             get_year(x[3]),
                             (float(x[2]), 1)
                         )
                     )

# Tính tổng điểm và số lượt
reduce_rdd = ratings_rdd.reduceByKey(
    lambda a, b: (
        a[0] + b[0],
        a[1] + b[1]
    )
)

# Tính average rating
avg_rdd = reduce_rdd.mapValues(
    lambda x: (
        x[0] / x[1],
        x[1]
    )
)

# Sắp xếp theo năm
results = avg_rdd.sortByKey().collect()

# kết quả
print(" PHÂN TÍCH ĐÁNH GIÁ THEO THỜI GIAN ")

for item in results:

    year = item[0]
    avg_rating = item[1][0]
    total_ratings = item[1][1]

    print(
        f"Year: {year}  "
        f"Average Rating: {avg_rating:.2f}  "
        f"Total Ratings: {total_ratings}"
    )

# Lưu file txt
with open("output_bai6.txt", "w", encoding="utf-8") as f:

    f.write(" PHÂN TÍCH ĐÁNH GIÁ THEO THỜI GIAN \n\n")

    for item in results:

        year = item[0]
        avg_rating = item[1][0]
        total_ratings = item[1][1]

        f.write(
            f"Year: {year}  "
            f"Average Rating: {avg_rating:.2f}  "
            f"Total Ratings: {total_ratings}\n"
        )

print("Đã lưu kết quả vào output_bai6.txt")
