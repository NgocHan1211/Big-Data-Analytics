
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

movies_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/movies.txt"
ratings1_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_1.txt"
ratings2_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_2.txt"

movies = sc.textFile(movies_path)
movies_rdd = movies.map(lambda line: line.split(",")) \
                   .map(lambda x: (x[0], x[1]))

ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)

ratings = ratings1.union(ratings2)

# MovieID -> (Rating, 1)
ratings_map = ratings.map(lambda line: line.split(",")) \
                     .map(lambda x: (x[1], (float(x[2]), 1)))

# Tính tổng điểm và số lượt rating
ratings_reduce = ratings_map.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# Tính average rating
ratings_avg = ratings_reduce.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

# Join với tên phim
movie_stats = movies_rdd.join(ratings_avg)

# Kết quả
results = movie_stats.collect()
print("ĐIỂM TRUNG BÌNH CỦA PHIM")

for movie in results:

    movie_id = movie[0]
    title = movie[1][0]
    avg_rating = movie[1][1][0]
    total_count = movie[1][1][1]

    print(
        f"MovieID: {movie_id}  "
        f"Title: {title}  "
        f"Average Rating: {avg_rating:.2f}  "
        f"Total Ratings: {total_count}"
    )

# Phim rating cao nhất
top_movie = movie_stats.filter(
    lambda x: x[1][1][1] >= 5
).takeOrdered(
    1,
    key=lambda x: -x[1][1][0]
)

print("\nPHIM CÓ ĐIỂM CAO NHẤT")

for movie in top_movie:

    print(f"MovieID: {movie[0]}")
    print(f"Title: {movie[1][0]}")
    print(f"Average Rating: {movie[1][1][0]:.2f}")
    print(f"Total Ratings: {movie[1][1][1]}")

with open("output_bai1.txt", "w", encoding="utf-8") as f:

    f.write("ĐIỂM TRUNG BÌNH VÀ SỐ LƯỢT ĐÁNH GIÁ\n\n")

    for movie in results:

        movie_id = movie[0]
        title = movie[1][0]
        avg_rating = movie[1][1][0]
        total_count = movie[1][1][1]

        f.write(
            f"MovieID: {movie_id}  "
            f"Title: {title}  "
            f"Average Rating: {avg_rating:.2f}  "
            f"Total Ratings: {total_count}\n"
        )

    f.write("\nPHIM CÓ ĐIỂM TRUNG BÌNH CAO NHẤT\n\n")

    for movie in top_movie:

        f.write(f"MovieID: {movie[0]}\n")
        f.write(f"Title: {movie[1][0]}\n")
        f.write(f"Average Rating: {movie[1][1][0]:.2f}\n")
        f.write(f"Total Ratings: {movie[1][1][1]}\n")

print("Đã lưu kết quả vào output_bai1.txt")
