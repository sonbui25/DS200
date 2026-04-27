from pyspark.sql import SparkSession

# 1. KHOI TAO SPARK
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Exercise_2")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext

# 2. DUONG DAN DATA
movies_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/movies.txt"
ratings_1_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_1.txt"
ratings_2_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_2.txt"
local_log_path = "/home/sonbui13/DS200/Lab_3/results/ex2/ex2_result.txt"


# 3. TAO MAP MovieID -> List[Genres]
def parse_movie_genres(line):
    movie_id, _, genres = line.split(",", 2)
    return int(movie_id), genres.split("|")


movie_genres_map = (
    sc.textFile(movies_path)
    .map(parse_movie_genres)
    .collectAsMap()
)
bc_movie_genres = sc.broadcast(movie_genres_map)

# 4. DOI RATINGS THANH (Genre, (Rating, 1))
def parse_rating_to_genres(line):
    user_id, movie_id, rating, timestamp = line.split(",")
    _ = user_id, timestamp
    movie_id = int(movie_id)
    rating = float(rating)
    genres = bc_movie_genres.value.get(movie_id, [])
    return [(genre, (rating, 1)) for genre in genres]


def sum_ratings(a, b):
    return a[0] + b[0], a[1] + b[1]


def calc_avg_and_count(total_and_count):
    total_rating, total_count = total_and_count
    avg_rating = total_rating / total_count
    return avg_rating, total_count


def sort_by_avg_desc(genre_row):
    # Sap xep giam dan theo diem trung binh cua tung the loai
    _, (avg_rating, _) = genre_row
    return avg_rating


genre_avg_rdd = (
    sc.textFile(ratings_1_path)
    .union(sc.textFile(ratings_2_path))
    .flatMap(parse_rating_to_genres)
    .reduceByKey(sum_ratings)
    .mapValues(calc_avg_and_count)
    .sortBy(sort_by_avg_desc, ascending=False)
)


# 5. IN KET QUA + GHI LOG
try:
    with open(local_log_path, "w", encoding="utf-8") as f:
        print("--- DIEM TRUNG BINH THEO THE LOAI ---")
        f.write("--- DIEM TRUNG BINH THEO THE LOAI ---\n")

        for genre, (avg, count) in genre_avg_rdd.toLocalIterator():
            print(f"Genre: {genre}, AvgRating: {avg:.2f}, Count: {count}")
            f.write(f"Genre: {genre}, AvgRating: {avg:.2f}, Count: {count}\n")

    print(f"Thanh cong! Kiem tra file tai: {local_log_path}")
except Exception as e:
    print(f"Loi khi ghi file: {e}")

spark.stop()
