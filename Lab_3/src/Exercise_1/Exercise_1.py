from pyspark.sql import SparkSession

# 1. KHỞI TẠO SPARK
# Sử dụng master("local[*]") để tận dụng toàn bộ CPU Core
# Giảm shuffle.partitions xuống 8 để tránh nghẽn I/O trên WSL
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab3_Exercise_1")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext

# Đường dẫn HDFS và Local
movies_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/movies.txt"
ratings_1_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_1.txt"
ratings_2_path = "hdfs://localhost:9000/user/sonbui13/lab3/input/ratings_2.txt"
local_log_path = "/home/sonbui13/DS200/Lab_3/results/ex1/ex1_result.txt"

# Số lượng phân vùng đồng nhất (Nên bằng số Core CPU)
num_p = 8


# BƯỚC 1: XỬ LÝ MOVIES (MAP)

def parse_movies(line):
    parts = line.split(",")
    return (int(parts[0]), parts[1]) # (MovieID, Title)

# Thực hiện Map và Phân vùng sẵn để Join cho nhanh
movies_rdd = sc.textFile(movies_path) \
               .map(parse_movies) \
               .partitionBy(num_p) \
               .cache()


# BƯỚC 2: XỬ LÝ RATINGS (UNION -> MAP -> REDUCE)


# 2.1 Union & Map
def parse_ratings(line):
    parts = line.split(",")
    # (MovieID, (Rating, 1)) -> 1 để tí nữa đếm tổng lượt đánh giá
    _ , movie_id, rating, _ = parts
    return (int(movie_id), (float(rating), 1))

ratings_raw = sc.textFile(ratings_1_path).union(sc.textFile(ratings_2_path))
ratings_mapped = ratings_raw.map(parse_ratings)

# 2.2 Reduce: Tính tổng điểm và tổng số lượt đánh giá theo MovieID
def sum_ratings(a, b):
    # a, b là (rating, count)
    total_rating, total_count = a
    rating, count = b
    return (total_rating + rating, total_count + count)

# Ép numPartitions ở đây để dữ liệu được gom nhóm sẵn
ratings_reduced = ratings_mapped.reduceByKey(sum_ratings, numPartitions=num_p)

# 2.3 Map: Tính trung bình cộng
def calc_avg(row):
    movie_id, (total_score, total_count) = row
    avg = total_score / total_count
    return (movie_id, (avg, total_count))

ratings_final = ratings_reduced.map(calc_avg)


# BƯỚC 3: KẾT HỢP DỮ LIỆU (JOIN)

# Kết quả có dạng: (MovieID, (Title, (AverageRating, Count)))
movie_title_rating_rdd = movies_rdd.join(ratings_final).cache()


# BƯỚC 4: GHI KẾT QUẢ RA FILE LOCAL (LOG)
try:
    with open(local_log_path, "w", encoding="utf-8") as f:
        print("--- DANH SÁCH PHIM - ĐÁNH GIÁ TRUNG BÌNH - SỐ LƯỢNG ĐÁNH GIÁ ---")
        f.write("--- DANH SÁCH PHIM - ĐÁNH GIÁ TRUNG BÌNH - SỐ LƯỢNG ĐÁNH GIÁ ---\n")

        # toLocalIterator giúp lấy từng dòng về Driver, tránh treo RAM
        for m_id, (title, (avg, cnt)) in movie_title_rating_rdd.toLocalIterator():
            print(f"MovieID: {m_id}, Title: {title}, Avg: {avg:.2f}, Count: {cnt}")
            f.write(f"MovieID: {m_id}, Title: {title}, Avg: {avg:.2f}, Count: {cnt}\n")
            
        # Tìm phim cao nhất (Lọc >= 5 lượt đánh giá)
        filtered = movie_title_rating_rdd.filter(lambda x: x[1][1][1] >= 5)
        if not filtered.isEmpty():
            top = filtered.max(key=lambda x: x[1][1][0])
            print("--- PHIM CÓ ĐIỂM TRUNG BÌNH CAO NHAT (>= 5 LUOT DANH GIA) ---")
            print(f"Title: {top[1][0]}, ĐIỂM TRUNG BÌNH: {top[1][1][0]:.2f}, SỐ LƯỢNG ĐÁNH GIÁ: {top[1][1][1]}")
            f.write("\n--- PHIM CÓ ĐIỂM TRUNG BÌNH CAO NHAT (>= 5 LUOT DANH GIA) ---\n")
            f.write(f"Title: {top[1][0]}, ĐIỂM TRUNG BÌNH: {top[1][1][0]:.2f}, SỐ LƯỢNG ĐÁNH GIÁ: {top[1][1][1]}\n")

    print(f"Thành công! Kiểm tra file tại: {local_log_path}")
except Exception as e:
    print(f"Lỗi khi ghi file: {e}")

spark.stop()