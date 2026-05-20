
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

movies_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/movies.txt"
ratings1_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_1.txt"
ratings2_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_2.txt"

movies = sc.textFile(movies_path)

# MovieID -> List Genres
movies_rdd = movies.map(lambda line: line.split(",")) \
                   .map(lambda x: (x[0], x[2].split("|")))

# ratings
ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)

ratings = ratings1.union(ratings2)

# MovieID -> Rating
ratings_rdd = ratings.map(lambda line: line.split(",")) \
                     .map(lambda x: (x[1], float(x[2])))

# Join MovieID
joined_rdd = ratings_rdd.join(movies_rdd)

# Genre -> (Rating,1)
genre_rating = joined_rdd.flatMap(
    lambda x: [(genre, (x[1][0], 1)) for genre in x[1][1]]
)

# Tính tổng điểm và số lượt
genre_reduce = genre_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# Tính điểm trung bình
genre_avg = genre_reduce.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

# Kết quả
results = genre_avg.sortBy(
    lambda x: x[1][0],
    ascending=False
).collect()
print(" ĐIỂM TRUNG BÌNH THEO THỂ LOẠI ")

for genre in results:

    genre_name = genre[0]
    avg_rating = genre[1][0]
    total_count = genre[1][1]

    print(
        f"Genre: {genre_name}  "
        f"Average Rating: {avg_rating:.2f}  "
    )

# lưu output
with open("output_bai2.txt", "w", encoding="utf-8") as f:

    f.write(" ĐIỂM TRUNG BÌNH THEO THỂ LOẠI \n\n")

    for genre in results:

        genre_name = genre[0]
        avg_rating = genre[1][0]
        total_count = genre[1][1]

        f.write(
            f"Genre: {genre_name} "
            f"Average Rating: {avg_rating:.2f}\n"
        )

print("Đã lưu kết quả vào output_bai2.txt")
