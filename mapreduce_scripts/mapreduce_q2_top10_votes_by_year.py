#!/usr/bin/env python3
import sys
import json
class Mapper:
    def run(self):
        seen_movie_ids = set() 
        
        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:
                    continue

                movie = json.loads(line)
                
                # Truy cập bằng key (tên cột)
                movie_id = movie.get("movie_id") 
                
                # Chống trùng lặp 
                if movie_id in seen_movie_ids:
                    continue
                seen_movie_ids.add(movie_id) 

                title = movie.get("title", "").strip()
                year = int(movie.get("year", 0)) # Dùng key "year"
                votes = int(movie.get("votes", 0)) # Dùng key "votes"
                rating = float(movie.get("rating", 0)) # Dùng key "rating"
                
                if year <= 0 or votes <= 0:
                    continue
                
                print("{}\t{:010d}|{}|{:.1f}".format(year, votes, title, rating))
                sys.stdout.flush()
            except:
                continue
class Reducer:
    def run(self):
        current_year = None
        top_movies = []
        
        for line in sys.stdin:
            try:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue
                
                year = int(parts[0])
                value = parts[1]
                vote_parts = value.split("|")
                if len(vote_parts) != 3:
                    continue
                
                votes = int(vote_parts[0])
                title = vote_parts[1]
                rating = vote_parts[2]
                
                if current_year == year:
                    top_movies.append((votes, title, rating))
                else:
                    if current_year is not None:
                        self.emit_top10(current_year, top_movies)
                    current_year = year
                    top_movies = [(votes, title, rating)]
            except:
                continue
        
        if current_year is not None:
            self.emit_top10(current_year, top_movies)
    
    def emit_top10(self, year, movies):
        top10 = sorted(movies, key=lambda x: x[0], reverse=True)[:10]
        for rank, (votes, title, rating) in enumerate(top10, 1):
            print("{}\t{}\t{}\t{}\t{}".format(year, rank, title[:50], votes, rating))
            sys.stdout.flush()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "reduce":
        reducer = Reducer()
        reducer.run()
    else:
        mapper = Mapper()
        mapper.run()
