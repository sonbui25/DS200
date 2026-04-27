from datetime import datetime
from pyspark.sql import SparkSession

# 1. KHOI TAO SPARK
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Exercise_6")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

# 2. DUONG DAN DATA
ratings_1_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_1.txt"
ratings_2_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_2.txt"
local_log_path = "/home/sonbui13/DS200/Lab_3/results/ex6/ex6_result.txt"


# 3. CAC HAM PARSE
def timestamp_to_year(unix_timestamp):
    return datetime.utcfromtimestamp(unix_timestamp).year


def parse_rating_to_year(line):
    user_id, movie_id, rating, timestamp = line.split(",", 3)
    _ = user_id, movie_id
    year = timestamp_to_year(int(timestamp))
    return year, (float(rating), 1)


def sum_rating_count(left_value, right_value):
    left_rating, left_count = left_value
    right_rating, right_count = right_value
    return left_rating + right_rating, left_count + right_count


def calc_avg_from_total(row):
    year, total_and_count = row
    total_rating, total_count = total_and_count
    avg_rating = total_rating / total_count
    return year, (avg_rating, total_count)


def sort_by_year(row):
    year, _ = row
    return year


year_stats_rdd = (
    sc.textFile(ratings_1_path)
    .union(sc.textFile(ratings_2_path))
    .map(parse_rating_to_year)
    .reduceByKey(sum_rating_count)
    .map(calc_avg_from_total)
    .sortBy(sort_by_year, ascending=True)
)


def format_result_line(row):
    year, (avg_rating, total_count) = row
    return f"{year}\tAverage_Rating: {avg_rating:.2f}, Count: {total_count}"


# 4. IN KET QUA + GHI LOG
try:
    with open(local_log_path, "w", encoding="utf-8") as f:
        print("--- PHAN TICH DANH GIA THEO NAM ---")
        f.write("--- PHAN TICH DANH GIA THEO NAM ---\n")

        for row in year_stats_rdd.toLocalIterator():
            line = format_result_line(row)
            print(line)
            f.write(line + "\n")

    print(f"Thanh cong! Kiem tra file tai: {local_log_path}")
except Exception as e:
    print(f"Loi khi ghi file: {e}")

spark.stop()
