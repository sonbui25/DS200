from pyspark.sql import SparkSession

# 1. KHOI TAO SPARK
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Exercise_5")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

# 2. DUONG DAN DATA
users_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/users.txt"
occupation_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/occupation.txt"
ratings_1_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_1.txt"
ratings_2_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_2.txt"
local_log_path = "/home/sonbui13/DS200/Lab_3/results/ex5/ex5_result.txt"


# 3. CAC HAM PARSE
def parse_user_occupation(line):
    user_id, gender, age, occupation_id, zip_code = line.split(",", 4)
    _ = gender, age, zip_code
    return int(user_id), int(occupation_id)


def parse_occupation_name(line):
    occupation_id, occupation_name = line.split(",", 1)
    return int(occupation_id), occupation_name


def parse_rating(line):
    user_id, movie_id, rating, timestamp = line.split(",", 3)
    _ = movie_id, timestamp
    return int(user_id), float(rating)


# 4. BROADCAST MAP
user_occupation_map = dict(sc.textFile(users_path).map(parse_user_occupation).collect())
occupation_name_map = dict(sc.textFile(occupation_path).map(parse_occupation_name).collect())
bc_user_occupation = sc.broadcast(user_occupation_map)
bc_occupation_name = sc.broadcast(occupation_name_map)


# 5. DOI RATINGS THANH (OCCUPATION_NAME, (RATING, 1))
def map_rating_to_occupation(row):
    user_id, rating = row
    occupation_id = bc_user_occupation.value.get(user_id)
    if occupation_id is None:
        return []

    occupation_name = bc_occupation_name.value.get(occupation_id, f"Occupation_{occupation_id}")
    return [(occupation_name, (rating, 1))]


def sum_rating_count(left_value, right_value):
    left_rating, left_count = left_value
    right_rating, right_count = right_value
    return left_rating + right_rating, left_count + right_count


def calc_avg_from_total(row):
    occupation_name, total_and_count = row
    total_rating, total_count = total_and_count
    avg_rating = total_rating / total_count
    return occupation_name, (avg_rating, total_count)


def sort_by_occupation_name(row):
    occupation_name, _ = row
    return occupation_name


occupation_stats_rdd = (
    sc.textFile(ratings_1_path)
    .union(sc.textFile(ratings_2_path))
    .map(parse_rating)
    .flatMap(map_rating_to_occupation)
    .reduceByKey(sum_rating_count)
    .map(calc_avg_from_total)
    .sortBy(sort_by_occupation_name, ascending=True)
)


def format_result_line(row):
    occupation_name, (avg_rating, total_count) = row
    return f"{occupation_name}\tAverage_Rating: {avg_rating:.2f}, Count: {total_count}"


# 6. IN KET QUA + GHI LOG
try:
    with open(local_log_path, "w", encoding="utf-8") as f:
        print("--- PHAN TICH DANH GIA THEO OCCUPATION ---")
        f.write("--- PHAN TICH DANH GIA THEO OCCUPATION ---\n")

        for row in occupation_stats_rdd.toLocalIterator():
            line = format_result_line(row)
            print(line)
            f.write(line + "\n")

    print(f"Thanh cong! Kiem tra file tai: {local_log_path}")
except Exception as e:
    print(f"Loi khi ghi file: {e}")

spark.stop()
