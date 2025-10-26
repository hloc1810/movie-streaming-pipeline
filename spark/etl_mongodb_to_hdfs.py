from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, when, year as spark_year, to_date, size
from pyspark.sql.types import IntegerType, DoubleType

print("=" * 60)
print("🔄 ETL: MONGODB (Streaming Data) → HDFS")
print("=" * 60)

# Khởi tạo Spark với MongoDB
spark = SparkSession.builder \
    .appName("MovieETL_Unified") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/movies.movie_data") \
    .config("spark.mongodb.wri on.uri", "mongodb://mongodb:27017/movies.movies_silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Sessin đã được tạo!")

# Đọc từ MongoDB collection chứa data streaming
print("📖 Đọc dữ liệu từ MongoDB (movies.movie_data)...")
df = spark.read \
    .format("mongodb") \
    .load()

total_records = df.count()
print(f"📊 Tổng số records từ MongoDB: {total_records}")

# Hiển thị schema
print("\n📋 Schema từ MongoDB:")
df.printSchema()

# Chọn các cột cần thiết và làm sạch
print("\n🔧 Đang ETL và làm sạch dữ liệu...")

#Ép Kiểu dữ liệu
df_cleaned = df.select(
    col("id").cast(IntegerType()).alias("movie_id"), #đổi tên cột id thành cột movie_id
    col("title"),
    col("original_title"),
    col("year").cast(IntegerType()),#ép kiểu số nguyên
    col("rating").cast(DoubleType()),#ép kiểu số thực
    col("votes").cast(IntegerType()),#ép kiểu số nguyên
    col("genres"), 
    col("overview"),
    col("popularity").cast(DoubleType()), #ép kiểu số thực
    col("original_language"),
    col("release_date"),
    col("source"),
    col("fetched_at")
)

# Validate và làm sạch dữ liệu
df_validated = df_cleaned \
    .withColumn("year", 
                when(col("year").isNull(), 0)
                .when(col("year") < 1900, 0)
                .when(col("year") > 2030, 0)
                .otherwise(col("year"))) \
    .withColumn("rating", 
                when(col("rating").isNull(), 0.0)
                .when(col("rating") < 0, 0.0)
                .when(col("rating") > 10, 0.0)
                .otherwise(col("rating"))) \
    .withColumn("votes", 
                when(col("votes").isNull(), 0)
                .when(col("votes") < 0, 0)
                .otherwise(col("votes"))) \
    .filter(size(col("genres")) > 0)  # Chỉ giữ phim có ít nhất 1 thể loại

print(f"✅ Sau khi validate: {df_validated.count()} records")

# Mỗi thể loại tạo 1 record riêng
print("\n💥 Tách 1 phim → nhiều records")
df_exploded = df_validated \
    .withColumn("genre", explode(col("genres"))) \
    .select(
        "movie_id",
        "title",
        "original_title",
        "year",
        "rating",
        "votes",
        "genre",  
        "overview",
        "popularity",
        "original_language",
        "release_date",
        "source",
        "fetched_at"
    )

# kiểm tra dữ liệu sau transform
print("\n📝 Mẫu data sau transform:")
df_exploded.select("title", "year", "rating", "votes", "genre").show(20, truncate=False)

# Thống kê trước khi ghi
print("\n" + "=" * 60)
print("📊 THỐNG KÊ DATA")
print("=" * 60)


print(f"Số phim unique: {df_exploded.select('movie_id').distinct().count():,}")
print(f"Số genres unique: {df_exploded.select('genre').distinct().count()}")

# # Thống kê theo năm
# year_stats = df_exploded.groupBy("year").count().orderBy("year", ascending=False)
# print("\n📅 Phân bố theo năm (Top 10):")
# year_stats.show(10)

# Thống kê theo genre
# genre_stats = df_exploded.groupBy("genre").count().orderBy("count", ascending=False)
# print("\n🎭 Phân bố theo genre:")
# genre_stats.show(20)

# Rating statistics
# rating_stats = df_exploded.select(
#     "rating"
# ).describe()
# print("\n⭐ Thống kê rating:")
# rating_stats.show()

# Lưu vào HDFS dạng JSONL
hdfs_path_jsonl = "hdfs://namenode:9000/data/movies/silver/movies.jsonl"
print(f"\n💾 Đang ghi vào HDFS: {hdfs_path_jsonl}")

df_exploded.write \
    .mode("overwrite") \
    .json(hdfs_path_jsonl) 
print("✅ Đã ghi vào HDFS thành công!")
print("\n" + "=" * 60)
print("✅ ETL HOÀN TẤT!")
print("=" * 60)
print(f"\n📁 Output:")
print(f"   - HDFS Main Data: {hdfs_path_jsonl}")
print(f"\n🎯 Sẵn sàng cho MapReduce jobs!")
spark.stop()