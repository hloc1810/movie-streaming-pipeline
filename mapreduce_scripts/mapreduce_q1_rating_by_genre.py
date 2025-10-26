
#!/usr/bin/env python3
"""
MapReduce Q1: Tinh rating trung binh theo genre
"""

import sys
import json
class Mapper:
    def run(self):
        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:
                    continue
                
                movie = json.loads(line) 
                
                # Truy cập bằng key (tên cột)
                genre = movie.get("genre", "").strip() # Dùng key "genre"
                rating = float(movie.get("rating", 0)) # Dùng key "rating"
                
                if not genre or rating <= 0:
                    continue
                
                print("{}\t{}\t1".format(genre, rating))
                sys.stdout.flush()
                
            except Exception:
                continue

class Reducer:
    def run(self):
        current_genre = None
        total_rating = 0.0
        count = 0
        
        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split("\t")
                if len(parts) != 3:
                    continue
                
                genre = parts[0]
                rating = float(parts[1])
                cnt = int(parts[2])
                
                if current_genre == genre:
                    total_rating += rating
                    count += cnt
                else:
                    if current_genre is not None:
                        self.emit_result(current_genre, total_rating, count)
                    
                    current_genre = genre
                    total_rating = rating
                    count = cnt
                    
            except Exception:
                continue
        
        if current_genre is not None:
            self.emit_result(current_genre, total_rating, count)
    
    def emit_result(self, genre, total_rating, count):
        if count >= 50:
            avg_rating = total_rating / count
            print("{}\t{:.2f}\t{}".format(genre, avg_rating, count))
            sys.stdout.flush()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "reduce":
        reducer = Reducer()
        reducer.run()
    else:
        mapper = Mapper()
        mapper.run()
