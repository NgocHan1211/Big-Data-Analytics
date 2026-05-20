
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

movies_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/movies.txt"
ratings1_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_1.txt"
ratings2_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_2.txt"
users_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/users.txt"

movies = sc.textFile(movies_path)

# MovieID -> Title
movies_rdd = movies.map(lambda line: line.split(",")) \
                   .map(lambda x: (x[0], x[1]))

# users
users = sc.textFile(users_path)

# UserID -> Gender
users_rdd = users.map(lambda line: line.split(",")) \
                 .map(lambda x: (x[0], x[1]))

# ratings
ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)

ratings = ratings1.union(ratings2)

# UserID -> (MovieID, Rating)
ratings_rdd = ratings.map(lambda line: line.split(",")) \
                     .map(lambda x: (x[0], (x[1], float(x[2]))))

# Join ratings với users
joined_rdd = ratings_rdd.join(users_rdd)

# ((MovieID, Gender) -> (Rating,1))
movie_gender = joined_rdd.map(
    lambda x: (
        (x[1][0][0], x[1][1]),
        (x[1][0][1], 1)
    )
)

# Tính tổng điểm và số lượt
reduce_rdd = movie_gender.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# Tính average rating
avg_rdd = reduce_rdd.mapValues(
    lambda x: x[0] / x[1]
)

# Đổi format để join tên phim
movie_avg = avg_rdd.map(
    lambda x: (
        x[0][0],
        (x[0][1], x[1])
    )
)

# Join với movie title
final_rdd = movies_rdd.join(movie_avg)

# Kết quả
results = final_rdd.collect()

print(" ĐIỂM TRUNG BÌNH PHIM THEO GIỚI TÍNH ")

for item in results:

    movie_id = item[0]
    title = item[1][0]
    gender = item[1][1][0]
    avg_rating = item[1][1][1]

    print(
        f"MovieID: {movie_id}  "
        f"Title: {title}  "
        f"Gender: {gender}  "
        f"Average Rating: {avg_rating:.2f}"
    )

# Lưu file txt
with open("output_bai3.txt", "w", encoding="utf-8") as f:

    f.write(" ĐIỂM TRUNG BÌNH PHIM THEO GIỚI TÍNH \n\n")

    for item in results:

        movie_id = item[0]
        title = item[1][0]
        gender = item[1][1][0]
        avg_rating = item[1][1][1]

        f.write(
            f"MovieID: {movie_id}  "
            f"Title: {title}  "
            f"Gender: {gender}  "
            f"Average Rating: {avg_rating:.2f}\n"
        )

print("Đã lưu kết quả vào output_bai3.txt")
