#!/usr/bin/env python3
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
                rating = float(movie.get("rating", 0)) # Dùng key "rating"
                
                if rating <= 0:
                    continue
                
                bucket = self.get_bucket(rating)
                print("{}\t1".format(bucket))
                sys.stdout.flush()
            except:
                continue
    
    def get_bucket(self, rating):
        if rating < 2:
            return "0-2"
        elif rating < 4:
            return "2-4"
        elif rating < 6:
            return "4-6"
        elif rating < 8:
            return "6-8"
        else:
            return "8-10"

class Reducer:
    def run(self):
        bucket_counts = {}
        total = 0
        
        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split("\t")
                if len(parts) != 2:
                    continue
                
                bucket = parts[0]
                count = int(parts[1])
                
                bucket_counts[bucket] = bucket_counts.get(bucket, 0) + count
                total += count
            except:
                continue
        
        buckets_order = ["0-2", "2-4", "4-6", "6-8", "8-10"]
        for bucket in buckets_order:
            count = bucket_counts.get(bucket, 0)
            percentage = (count / total * 100) if total > 0 else 0
            print("{}\t{}\t{:.2f}".format(bucket, count, percentage))
            sys.stdout.flush()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "reduce":
        reducer = Reducer()
        reducer.run()
    else:
        mapper = Mapper()
        mapper.run()
