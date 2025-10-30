# 🎬 Movie Data Pipeline - Real-time Streaming & Big Data Analytics

Hệ thống xử lý và phân tích dữ liệu phim theo thời gian thực, sử dụng kiến trúc Lambda với Kafka, Spark Streaming, Hadoop MapReduce và MongoDB.

## 📋 Mục lục

- [Tổng quan](#-tổng-quan)
- [Kiến trúc hệ thống](#-kiến-trúc-hệ-thống)
- [Công nghệ sử dụng](#-công-nghệ-sử-dụng)
- [Yêu cầu hệ thống](#-yêu-cầu-hệ-thống)
- [Cài đặt và chạy](#-cài-đặt-và-chạy)
- [Các thành phần chính](#-các-thành-phần-chính)
- [MapReduce Jobs](#-mapreduce-jobs)
- [API Keys](#-api-keys)
- [Ports và Services](#-ports-và-services)


## 🎯 Tổng quan

Dự án xây dựng một data pipeline hoàn chỉnh để:
- **Thu thập** dữ liệu phim từ TMDB API theo thời gian thực
- **Streaming** dữ liệu qua Apache Kafka
- **Xử lý real-time** với Spark Structured Streaming
- **Lưu trữ** trong MongoDB (NoSQL)
- **ETL** từ MongoDB sang HDFS
- **Phân tích batch** với Hadoop MapReduce

## 🏗️ Kiến trúc hệ thống

```
TMDB API
    ↓
[Producer] → Kafka → [Spark Streaming] → MongoDB
                                              ↓
                                          [ETL Job]
                                              ↓
                                            HDFS
                                              ↓
                                      [MapReduce Jobs]
                                              ↓
                                         Analytics
```

### Luồng dữ liệu chi tiết:

1. **Data Ingestion (Real-time)**
   - Producer gọi TMDB API 
   - Gửi dữ liệu phim vào Kafka topic `movies`
   - Tự động chuyển đổi genre IDs thành tên

2. **Stream Processing**
   - Spark Streaming đọc từ Kafka
   - Validate và transform dữ liệu
   - Ghi vào MongoDB collection `movies.movie_data`

3. **Batch ETL**
   - Đọc dữ liệu từ MongoDB
   - Làm sạch và chuẩn hóa
   - Explode genres (1 phim → N records theo thể loại)
   - Lưu vào HDFS dạng JSONL

4. **Analytics**
   - MapReduce jobs phân tích trên HDFS
   - 3 bài toán phân tích chính

## 🛠️ Công nghệ sử dụng

| Công nghệ | Version | Vai trò |
|-----------|---------|---------|
| **Apache Kafka** | 7.3.2 | Message Queue |
| **Apache Spark** | 3.4.1 | Stream Processing & ETL |
| **Hadoop HDFS** | 3.2.1 | Distributed Storage |
| **Hadoop YARN** | 3.2.1 | Resource Manager |
| **MongoDB** | 6 | NoSQL Database |
| **Python** | 3.9 | Programming Language |
| **Docker** | Latest | Containerization |
| **Zookeeper** | 7.3.2 | Kafka Coordination |

## 💻 Yêu cầu hệ thống

- **Docker** & **Docker Compose** installed
- **Minimum**: 8GB RAM, 4 CPU cores
- **Recommended**: 16GB RAM, 8 CPU cores
- **Disk Space**: ~10GB free

## 🚀 Cài đặt và chạy

### 1. Clone repository và chuẩn bị

```bash
git clone <repository-url>
cd <project-directory>
```

### 2. Cấu hình TMDB API Key

Tạo file `.env` trong thư mục gốc:

```bash
TMDB_API_KEY="your_api_key_here"
```

Hoặc sử dụng API key mặc định đã có trong code (chỉ để test).

### 3. Khởi động toàn bộ hệ thống

```bash
docker-compose up -d
```

### 4. Kiểm tra trạng thái services

```bash
docker-compose ps
```

Chờ khoảng 2-3 phút để tất cả services khởi động hoàn tất.

### 5. Theo dõi logs

```bash
# Producer logs
docker logs -f movie-producer

# Spark Streaming logs
docker logs -f spark-submit

# Kafka logs
docker logs -f kafka
```

### 6. Chạy ETL từ MongoDB sang HDFS

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  /opt/spark/work-dir/etl_mongodb_to_hdfs.py
```

### 7. Chạy MapReduce jobs

**Job 1: Rating trung bình theo thể loại**

```bash
docker exec -it namenode bash -c "
  hdfs dfs -rm -r /output/q1 2>/dev/null || true
  hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -files /mapreduce_scripts/mapreduce_q1_rating_by_genre.py \
    -mapper 'python3 mapreduce_q1_rating_by_genre.py' \
    -reducer 'python3 mapreduce_q1_rating_by_genre.py reduce' \
    -input /data/movies/silver/movies.jsonl \
    -output /output/q1
  
  hdfs dfs -cat /output/q1/part-* > /mapreduce_scripts/results_q1.txt
"
```

**Job 2: Top 10 phim có lượt vote cao nhất theo năm**

```bash
docker exec -it namenode bash -c "
  hdfs dfs -rm -r /output/q2 2>/dev/null || true
  hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -files /mapreduce_scripts/mapreduce_q2_top10_votes_by_year.py \
    -mapper 'python3 mapreduce_q2_top10_votes_by_year.py' \
    -reducer 'python3 mapreduce_q2_top10_votes_by_year.py reduce' \
    -input /data/movies/silver/movies.jsonl \
    -output /output/q2
  
  hdfs dfs -cat /output/q2/part-* > /mapreduce_scripts/results_q2.txt
"
```

**Job 3: Phân phối phim theo khoảng rating**

```bash
docker exec -it namenode bash -c "
  hdfs dfs -rm -r /output/q3 2>/dev/null || true
  hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -files /mapreduce_scripts/mapreduce_q3_rating_buckets.py \
    -mapper 'python3 mapreduce_q3_rating_buckets.py' \
    -reducer 'python3 mapreduce_q3_rating_buckets.py reduce' \
    -input /data/movies/silver/movies.jsonl \
    -output /output/q3
  
  hdfs dfs -cat /output/q3/part-* > /mapreduce_scripts/results_q3.txt
"
```

### 8. Xem kết quả

```bash
# Trong container
docker exec -it namenode cat /mapreduce_scripts/results_q1.txt
docker exec -it namenode cat /mapreduce_scripts/results_q2.txt
docker exec -it namenode cat /mapreduce_scripts/results_q3.txt

# Copy ra host
docker cp namenode:/mapreduce_scripts/results_q1.txt ./
docker cp namenode:/mapreduce_scripts/results_q2.txt ./
docker cp namenode:/mapreduce_scripts/results_q3.txt ./
```

## 📦 Các thành phần chính

### 1. Movie Producer (`producer/movie_producer.py`)

**Chức năng:**
- Lấy dữ liệu phim từ TMDB API
- Chuyển đổi genre IDs thành tên
- Gửi message vào Kafka topic `movies`
- Tránh duplicate movies với set tracking

**Output format:**
```json
{
  "id": 12345,
  "title": "Movie Name",
  "year": 2024,
  "rating": 7.5,
  "votes": 1500,
  "genres": ["Action", "Adventure"],
  "popularity": 45.2,
  "source": "popular",
  "fetched_at": "2024-01-15 10:30:00"
}
```

### 2. Spark Streaming (`spark/spark_streaming_app.py`)

**Chức năng:**
- Đọc real-time stream từ Kafka
- Parse JSON và validate schema
- Transform dữ liệu
- Ghi vào MongoDB với timestamp

**Xử lý:**
- Batch processing mỗi micro-batch
- Checkpoint để fault-tolerance
- Display progress logs

### 3. ETL Job (`spark/etl_mongodb_to_hdfs.py`)

**Pipeline:**
1. Đọc từ MongoDB collection
2. Cast data types (IntegerType, DoubleType)
3. Validate data:
   - Year: 1900-2030
   - Rating: 0-10
   - Votes: >= 0
4. Explode genres (1 phim → nhiều records)
5. Ghi vào HDFS dạng JSONL

**Output schema:**
```
movie_id, title, original_title, year, rating, votes, 
genre, overview, popularity, original_language, 
release_date, source, fetched_at
```

### 4. MapReduce Scripts

**Location:** `mapreduce_scripts/`

**Đặc điểm:**
- Sử dụng Hadoop Streaming API
- Python implementation
- Class-based design (Mapper, Reducer)
- JSON parsing từ HDFS

## 📊 MapReduce Jobs

### Q1: Rating trung bình theo thể loại

**Output format:**
```
Genre    AvgRating    Count
Action   7.25         1500
Drama    7.89         2300
```

**Logic:**
- Mapper: Emit (genre, rating, 1)
- Reducer: Tính avg, filter >= 50 movies

### Q2: Top 10 phim vote cao nhất mỗi năm

**Output format:**
```
Year  Rank  Title          Votes    Rating
2024  1     Movie A        50000    8.5
2024  2     Movie B        45000    8.3
```

**Logic:**
- Mapper: Emit (year, votes|title|rating)
- Reducer: Sort by votes, lấy top 10

### Q3: Phân phối rating

**Output format:**
```
Bucket   Count   Percentage
0-2      150     2.5%
2-4      500     8.3%
4-6      2000    33.3%
6-8      2500    41.7%
8-10     850     14.2%
```

**Logic:**
- Mapper: Phân loại rating vào bucket
- Reducer: Đếm và tính %

## 🔑 API Keys

Dự án sử dụng TMDB API. Để lấy API key:

1. Đăng ký tại: https://www.themoviedb.org/signup
2. Vào Settings → API
3. Request API key (miễn phí)
4. Thêm vào file `.env`

**Rate Limits:**
- 40 requests/10 seconds
- Producer đã config delay 5s giữa các requests

## 🌐 Ports và Services

| Service | Port | URL | Mô tả |
|---------|------|-----|-------|
| **Hadoop NameNode UI** | 9870 | http://localhost:9870 | HDFS Web UI |
| **YARN ResourceManager** | 8088 | http://localhost:8088 | YARN Jobs UI |
| **Spark Master UI** | 8080 | http://localhost:8080 | Spark Cluster UI |
| **Kafka** | 9092, 29092 | - | Kafka Broker |
| **MongoDB** | 27017 | - | Database |
| **Zookeeper** | 2181 | - | Coordination |

## 🔍 Monitoring & Debugging

### 1. Kiểm tra Kafka topic

```bash
# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic movies \
  --from-beginning \
  --max-messages 10
```

### 2. Kiểm tra HDFS

```bash
# List files
docker exec -it namenode hdfs dfs -ls /data/movies/silver/

# Read file content
docker exec -it namenode hdfs dfs -cat /data/movies/silver/movies.jsonl/part-*.json | head -20

# HDFS report
docker exec -it namenode hdfs dfsadmin -report
```

### 3. Kiểm tra MongoDB

```bash
# Access MongoDB shell
docker exec -it mongodb mongosh

# Inside mongosh:
use movies
db.movie_data.countDocuments()
db.movie_data.find().limit(5).pretty()
```

### 4. Check Spark Jobs

Truy cập: http://localhost:8080
- Xem workers status
- Running applications
- Completed jobs

### 5. Check YARN Jobs

Truy cập: http://localhost:8088
- Active applications
- Job history
- Resource usage

### Clear all data và restart

```bash
docker-compose down -v
rm -rf data/
docker-compose up -d
```

## 📂 Cấu trúc thư mục

```
.
├── docker-compose.yml           # Orchestration
├── .env                         # API keys
├── hadoop.env                   # Hadoop config
├── producer/
│   ├── Dockerfile
│   └── movie_producer.py       # Kafka Producer
├── spark/
│   ├── spark_streaming_app.py  # Real-time processing
│   └── etl_mongodb_to_hdfs.py  # Batch ETL
├── mapreduce_scripts/
│   ├── mapreduce_q1_rating_by_genre.py
│   ├── mapreduce_q2_top10_votes_by_year.py
│   └── mapreduce_q3_rating_buckets.py
├── data/                        # HDFS data (gitignored)
│   ├── namenode/
│   ├── datanode/
│   └── historyserver/

```

## 🎓 Học gì từ dự án này?

1. **Lambda Architecture**: Kết hợp stream + batch processing
2. **Data Engineering**: ETL pipelines, data validation
3. **Distributed Systems**: Hadoop, Spark cluster
4. **Stream Processing**: Kafka + Spark Streaming
5. **NoSQL**: MongoDB document store
6. **Big Data**: MapReduce programming model
7. **DevOps**: Docker, containerization


**Created with ❤️ for Data Engineer Learning**
