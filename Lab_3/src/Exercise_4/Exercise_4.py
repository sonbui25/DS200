from pyspark.sql import SparkSession

# 1. KHOI TAO SPARK
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Exercise_4")
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
local_log_path = "/home/sonbui13/DS200/Lab_3/results/ex4/ex4_result.txt"


# 3. CAC HAM PARSE CO BAN
def parse_movie_title(line):
    movie_id, title, genres = line.split(",", 2)
    _ = genres
    return int(movie_id), title


def map_age_to_group(age):
    # Chia nhom tuoi dung format output mong muon
    if age <= 18:
        return "0-18"
    if age <= 35:
        return "18-35"
    if age <= 50:
        return "35-50"
    return "50+"


def parse_user_age_group(line):
    user_id, gender, age, occupation, zip_code = line.split(",", 4)
    _ = gender, occupation, zip_code
    return int(user_id), map_age_to_group(int(age))


def parse_rating(line):
    user_id, movie_id, rating, timestamp = line.split(",", 3)
    _ = timestamp
    return int(user_id), (int(movie_id), float(rating))


# 4. BROADCAST MAP DE GIAM CHI PHI JOIN
movie_title_map = dict(sc.textFile(movies_path).map(parse_movie_title).collect())
user_age_group_map = dict(sc.textFile(users_path).map(parse_user_age_group).collect())
bc_movie_title = sc.broadcast(movie_title_map)
bc_user_age_group = sc.broadcast(user_age_group_map)


# 5. DOI RATINGS THANH KEY (MOVIEID, AGE_GROUP)
def map_rating_to_movie_age_group(row):
    user_id, (movie_id, rating) = row
    age_group = bc_user_age_group.value.get(user_id)
    if age_group is None:
        return []
    return [((movie_id, age_group), (rating, 1))]


def sum_rating_count(left_value, right_value):
    left_rating, left_count = left_value
    right_rating, right_count = right_value
    return left_rating + right_rating, left_count + right_count


def calc_avg_from_total(row):
    movie_age_group_key, total_and_count = row
    total_rating, total_count = total_and_count
    avg_rating = total_rating / total_count
    return movie_age_group_key, (avg_rating, total_count)


movie_age_group_avg_rdd = (
    sc.textFile(ratings_1_path)
    .union(sc.textFile(ratings_2_path))
    .map(parse_rating)
    .flatMap(map_rating_to_movie_age_group)
    .reduceByKey(sum_rating_count)
    .map(calc_avg_from_total)
)


def format_rating_text(rating_value):
    if rating_value is None:
        return "NA"
    return f"{rating_value:.2f}"


def format_movie_age_line(title, age_group_data):
    return (
        f"{title}\t"
        f"[0-18: {format_rating_text(age_group_data['0-18'])}, "
        f"18-35: {format_rating_text(age_group_data['18-35'])}, "
        f"35-50: {format_rating_text(age_group_data['35-50'])}, "
        f"50+: {format_rating_text(age_group_data['50+'])}]"
    )


# 6. IN KET QUA + GHI LOG
try:
    with open(local_log_path, "w", encoding="utf-8") as f:
        print("--- DIEM TRUNG BINH THEO PHIM VA NHOM TUOI ---")
        f.write("--- DIEM TRUNG BINH THEO PHIM VA NHOM TUOI ---\n")

        movie_age_summary = {}
        for row in movie_age_group_avg_rdd.toLocalIterator():
            (movie_id, age_group), (avg_rating, total_count) = row
            _ = total_count
            if movie_id not in movie_age_summary:
                movie_age_summary[movie_id] = {
                    "title": bc_movie_title.value.get(movie_id, "N/A"),
                    "0-18": None,
                    "18-35": None,
                    "35-50": None,
                    "50+": None,
                }
            movie_age_summary[movie_id][age_group] = avg_rating

        for movie_id in sorted(movie_age_summary):
            movie_data = movie_age_summary[movie_id]
            line = format_movie_age_line(movie_data["title"], movie_data)
            print(line)
            f.write(line + "\n")

    print(f"Thanh cong! Kiem tra file tai: {local_log_path}")
except Exception as e:
    print(f"Loi khi ghi file: {e}")

spark.stop()
