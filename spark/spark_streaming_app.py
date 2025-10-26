from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, BooleanType, ArrayType

print("=" * 60)
print("🎬 SPARK STREAMING App start")
print("=" * 60)

spark = SparkSession.builder \
    .appName("MovieStreamingPipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "4") \
    .config("spark.executor.cores", "2") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/movies.movie_data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Đang đọc dữ liệu từ Kafka...")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "movies") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

schema = StructType() \
    .add("id", IntegerType()) \
    .add("title", StringType()) \
    .add("original_title", StringType()) \
    .add("year", IntegerType()) \
    .add("rating", DoubleType()) \
    .add("votes", IntegerType()) \
    .add("genres", ArrayType(StringType())) \
    .add("genre_ids", ArrayType(IntegerType())) \
    .add("overview", StringType()) \
    .add("popularity", DoubleType()) \
    .add("adult", BooleanType()) \
    .add("original_language", StringType()) \
    .add("poster_path", StringType()) \
    .add("backdrop_path", StringType()) \
    .add("release_date", StringType()) \
    .add("source", StringType()) \
    .add("fetched_at", StringType())

movie_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_at", current_timestamp())


movie_df_display = movie_df \
    .withColumnRenamed("processed_at", "Thời gian xử lý")

def write_to_mongodb(batch_df, batch_id):
            # Ghi vào MongoDB
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .save()
            
            count = batch_df.count()
            print(f"\n{'='*80}")
            print(f"✅ Batch {batch_id}: Đã ghi {count} phim vào MongoDB")
            print(f"{'='*80}")
            
            # Hiển thị thông tin chi tiết
            movies = batch_df.select("title", "genres").collect()
            for idx, movie in enumerate(movies[:10], 1): 
                genres_str = ", ".join(movie['genres'][:3])
                print(f"   🎬 [{idx:2d}] {movie['title'][:45]:<45}")
            
            if count > 10:
                print(f"   ... và {count - 10} phim khác")
            
            print(f"{'='*80}\n")
# Ghi vào MongoDB 
query = movie_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_mongodb) \
    .option("checkpointLocation", "/tmp/spark-checkpoints-movies") \
    .start()

print("📊 Đang lắng nghe Kafka topic 'movies'...")
query.awaitTermination()