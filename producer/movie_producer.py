import json
import time
import sys
import os
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

print("=" * 60)
print("🎬 MOVIE PRODUCER STARTING")
print("=" * 60)

KAFKA_BROKER = "kafka:9092"
TOPIC = "movies"

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "f117dbf2f62bc3d592f5c8651196add5")

POPULAR_URL = "https://api.themoviedb.org/3/movie/popular"
GENRE_URL = "https://api.themoviedb.org/3/genre/movie/list"

print(f" Kafka Broker: {KAFKA_BROKER}")
print(f" Topic: {TOPIC}")

#Chuyển id thể loại thành tên thể loại
def get_genre_mapping():
    response = requests.get(GENRE_URL, params={"api_key": TMDB_API_KEY})
    if response.status_code == 200:
        data = response.json()
        genres = data.get("genres", [])
        # Tạo từ điển vd: {28: "Action", 12: "Adventure", ...}
        return {g["id"]: g["name"] for g in genres}
    return {}

print("🔄 Đang lấy genre mapping...", flush=True)
GENRE_MAPPING = get_genre_mapping()
print(f"✅ Đã lấy {len(GENRE_MAPPING)} genres", flush=True)


print("🔄 Đang kết nối tới Kafka...", flush=True)

max_retries = 15
retry_count = 0
producer = None

while retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000
        )
        print("✅ Đã kết nối tới Kafka!", flush=True)
        break
    except Exception as e:
        retry_count += 1
        print(f"⚠️ Thử lại ({retry_count}/{max_retries}): {str(e)}", flush=True)
        time.sleep(5)

if producer is None:
    print("❌ KHÔNG THỂ KẾT NỐI!", flush=True)
    sys.exit(1)

print("🎬 Bắt đầu gửi dữ liệu phim...", flush=True)
message_count = 0
page = 1

# Set để tránh gửi trùng phim
sent_movie_ids = set()

while True:
    try:
        api_url = POPULAR_URL
        api_type = "Popular"
        
        response = requests.get(
            api_url,
            params={
                "api_key": TMDB_API_KEY,
                "page": page, 
                "language": "en-US" 
            },
            timeout=10
        )       
        if response.status_code == 200:
            data = response.json()
            movies = data.get("results", []) 
            
            if movies:
                for movie in movies:
                    movie_id = movie.get("id")
                    
                    # Bỏ qua nếu đã gửi phim này rồi
                    if movie_id in sent_movie_ids:
                        continue
                    
                    sent_movie_ids.add(movie_id)
                    
                    # Chuyển id thể loại thành tên thể loại
                    genre_ids = movie.get("genre_ids", [])
                    genres = [GENRE_MAPPING.get(gid, f"Unknown_{gid}") for gid in genre_ids]
                    
                    # Extract year từ release_date
                    release_date = movie.get("release_date", "")
                    year = 0
                    if release_date:
                        try:
                            year = int(release_date.split("-")[0])
                        except:
                            year = 0
                    
                    # Chuẩn bị data phù hợp cho ETL
                    movie_data = {
                        # Thông tin cơ bản
                        "id": movie_id,
                        "title": movie.get("title", ""),
                        "original_title": movie.get("original_title", ""),
                        
                        # Dữ liệu phân tích 
                        "year": year,
                        "rating": round(movie.get("vote_average", 0.0), 1),
                        "votes": movie.get("vote_count", 0),
                        "genres": genres,  
                        "genre_ids": genre_ids,  
                        
                        # Thông tin bổ sung
                        "overview": movie.get("overview", ""),
                        "popularity": movie.get("popularity", 0.0),
                        "adult": movie.get("adult", False),
                        "original_language": movie.get("original_language", ""),
                        "poster_path": movie.get("poster_path", ""),
                        "backdrop_path": movie.get("backdrop_path", ""),
                        "release_date": release_date,
                        
                        # Metadata
                        "source": api_type.lower().replace(" ", "_"),
                        "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    # Gửi lên Kafka
                    future = producer.send(TOPIC, movie_data)
                    record_metadata = future.get(timeout=10)
                    message_count += 1
                    
                    genres_str = ", ".join(genres[:1])  
                    print(f"✅ #{message_count}: {movie_data['title'][:40]:<40}", flush=True)
                    
                    time.sleep(2)
                
                page += 1                    
            else:
                print("⚠️ Không có dữ liệu phim", flush=True)
        else:
            print(f"❌ API Error: {response.status_code}", flush=True)
            
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}", flush=True)
    
    time.sleep(5)