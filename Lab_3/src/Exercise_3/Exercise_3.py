from pyspark.sql import SparkSession

# 1. KHOI TAO SPARK
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Exercise_3")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

# 2. DUONG DAN DATA
movies_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/movies.txt"
users_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/users.txt"
ratings_1_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_1.txt"
ratings_2_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_2.txt"
local_log_path = "/home/sonbui13/DS200/Lab_3/results/ex3/ex3_result.txt"


# 3. PARSE DU LIEU DAU VAO
def parse_movie_title(line):
    movie_id, title, genres = line.split(",", 2)
    _ = genres
    return int(movie_id), title


def parse_user_gender(line):
    user_id, gender, age, occupation, zip_code = line.split(",", 4)
    _ = age, occupation, zip_code
    return int(user_id), gender


def parse_rating_by_user(line):
    user_id, movie_id, rating, timestamp = line.split(",", 3)
    _ = timestamp
    return int(user_id), (int(movie_id), float(rating))


# 4. TAO BROADCAST MAP (THAY JOIN NANG)
movie_title_map = dict(sc.textFile(movies_path).map(parse_movie_title).collect())
user_gender_map = dict(sc.textFile(users_path).map(parse_user_gender).collect())
bc_movie_title = sc.broadcast(movie_title_map)
bc_user_gender = sc.broadcast(user_gender_map)


# 5. DOI RATINGS THANH KEY (MOVIEID, GENDER)
def map_rating_to_movie_gender(row):
    user_id, (movie_id, rating) = row
    gender = bc_user_gender.value.get(user_id)
    if gender is None:
        return []
    return [((movie_id, gender), (rating, 1))]


def sum_rating_count(left_value, right_value):
    left_rating, left_count = left_value
    right_rating, right_count = right_value
    return left_rating + right_rating, left_count + right_count


def calc_avg_from_total(row):
    movie_gender_key, total_and_count = row
    total_rating, total_count = total_and_count
    avg_rating = total_rating / total_count
    return movie_gender_key, (avg_rating, total_count)


# 6. PIPELINE CHINH
movie_gender_avg_rdd = (
    sc.textFile(ratings_1_path)
    .union(sc.textFile(ratings_2_path))
    .map(parse_rating_by_user)
    .flatMap(map_rating_to_movie_gender)
    .reduceByKey(sum_rating_count)
    .map(calc_avg_from_total)
)


def format_rating_text(rating_value):
    if rating_value is None:
        return "N/A"
    return f"{rating_value:.2f}"


def format_movie_gender_line(title, male_avg, female_avg):
    return (
        f"{title}\t"
        f"Male_Average_Rating: {format_rating_text(male_avg)}, "
        f"Female_Average_Rating: {format_rating_text(female_avg)}"
    )


# 7. IN KET QUA + GHI LOG
try:
    with open(local_log_path, "w", encoding="utf-8") as f:
        print("--- DIEM TRUNG BINH THEO PHIM VA GIOI TINH ---")
        f.write("--- DIEM TRUNG BINH THEO PHIM VA GIOI TINH ---\n")

        # Gom ket qua M/F vao cung mot dong theo tung phim
        movie_gender_summary = {}
        for row in movie_gender_avg_rdd.toLocalIterator():
            (movie_id, gender), (avg_rating, total_count) = row
            _ = total_count
            if movie_id not in movie_gender_summary:
                movie_gender_summary[movie_id] = {
                    "title": bc_movie_title.value.get(movie_id, "N/A"),
                    "M": None,
                    "F": None,
                }
            movie_gender_summary[movie_id][gender] = avg_rating

        for movie_id in sorted(movie_gender_summary):
            movie_data = movie_gender_summary[movie_id]
            line = format_movie_gender_line(
                movie_data["title"],
                movie_data["M"],
                movie_data["F"],
            )
            print(line)
            f.write(line + "\n")

    print(f"Thanh cong! Kiem tra file tai: {local_log_path}")
except Exception as e:
    print(f"Loi khi ghi file: {e}")

spark.stop()
