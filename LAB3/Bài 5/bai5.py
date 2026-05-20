
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

users_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/users.txt"
ratings1_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_1.txt"
ratings2_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/ratings_2.txt"
occupation_path = "/kaggle/input/datasets/kghangco/pyspark-ds200/occupation.txt"

# occupation
occupation = sc.textFile(occupation_path)

# OccupationID -> OccupationName
occupation_rdd = occupation.map(lambda line: line.split(",")) \
                           .map(lambda x: (x[0], x[1]))

# users
users = sc.textFile(users_path)

# UserID -> OccupationID
users_rdd = users.map(lambda line: line.split(",")) \
                 .map(lambda x: (x[0], x[3]))

# Join users với occupation
# OccupationID -> (UserID, OccupationName)

user_occ = users_rdd.map(
    lambda x: (x[1], x[0])
)

joined_occ = user_occ.join(occupation_rdd)

# UserID -> OccupationName
user_occupation_rdd = joined_occ.map(
    lambda x: (x[1][0], x[1][1])
)

# ratings
ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)

ratings = ratings1.union(ratings2)

# UserID -> Rating
ratings_rdd = ratings.map(lambda line: line.split(",")) \
                     .map(lambda x: (x[0], float(x[2])))

# Join ratings với occupation
joined_rdd = ratings_rdd.join(user_occupation_rdd)

# Occupation -> (Rating,1)
occupation_rating = joined_rdd.map(
    lambda x: (
        x[1][1],
        (x[1][0], 1)
    )
)

# Tính tổng điểm và số lượt
reduce_rdd = occupation_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# Tính average rating
avg_rdd = reduce_rdd.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

# Kết quả
results = avg_rdd.collect()

print(" ĐÁNH GIÁ THEO OCCUPATION ")

for item in results:

    occupation_name = item[0]
    avg_rating = item[1][0]
    total_ratings = item[1][1]

    print(
        f"Occupation: {occupation_name}  "
        f"Average Rating: {avg_rating:.2f}  "
        f"Total Ratings: {total_ratings}"
    )

# Lưu file txt
with open("output_bai5.txt", "w", encoding="utf-8") as f:

    f.write(" ĐÁNH GIÁ THEO OCCUPATION \n\n")

    for item in results:

        occupation_name = item[0]
        avg_rating = item[1][0]
        total_ratings = item[1][1]

        f.write(
            f"Occupation: {occupation_name}  "
            f"Average Rating: {avg_rating:.2f}  "
            f"Total Ratings: {total_ratings}\n"
        )

print("Đã lưu kết quả vào output_bai5.txt")
