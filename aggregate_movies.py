import redis
import json
import ast

redis_db = redis.Redis(host='localhost', port=6379, db=0)

def aggregate_data():

    # CLEAN old aggregation keys to avoid type conflicts
    print("Cleaning up old aggregation keys...")
    for prefix in ["genre:", "actor:", "year:", "top_rated:"]:
        for key in redis_db.scan_iter(f"{prefix}*"):
            redis_db.delete(key)
    print("Cleanup complete.")

    # SCAN all movies
    LIMIT = 5000  # Adjust limit as needed
    processed = 0
    cursor = 0
    print("Starting data aggregation...")

    while True:
        cursor, keys = redis_db.scan(cursor=cursor, match="movie:*", count=1000)
        for key in keys:
            if processed >= LIMIT:
                break
            data = redis_db.get(key)
            if not data:
                continue

            try:
                movie = json.loads(data)
                movie_id = str(movie.get("id"))
                vote_avg = float(movie.get("vote_average", 0))

                # Parse release year
                year_raw = movie.get("release_year")
                year = None
                try:
                    if isinstance(year_raw, (int, float)):
                        year = str(int(year_raw))
                    elif isinstance(year_raw, str) and year_raw.strip().isdigit():
                        year = str(int(year_raw.strip()))
                except:
                    year = None

                # Parse main actor
                main_cast = movie.get("Star1")
                # Parse genres list safely
                genres_raw = movie.get("genres_list", "[]")
                try:
                    genres = ast.literal_eval(genres_raw) if isinstance(genres_raw, str) else genres_raw
                    genres = [str(g).strip() for g in genres if g is not None]
                except:
                    genres = []

                if movie_id:
                    movie_key = f"movie:{movie_id}"

                    # Actor aggregation
                    if main_cast and isinstance(main_cast, str):
                        actor_clean = main_cast.strip()
                        if actor_clean:
                            redis_db.sadd(f"actor:{actor_clean}", movie_key)

                    # Genre aggregation and top rated zset
                    for genre in genres:
                        if genre:
                            redis_db.sadd(f"genre:{genre}", movie_key)
                            redis_db.zadd(f"top_rated:{genre}", {movie_key: vote_avg})

                    # Year aggregation
                    if year:
                        redis_db.sadd(f"year:{year}", movie_key)

                processed += 1
                if processed % 100 == 0:
                    print(f"Processed {processed} movies...")

            except Exception as e:
                print(f"Error processing {key.decode()}: {e}")

        if cursor == 0 or processed >= LIMIT:
            break

    print(f"✅ Aggregation completed for {processed} movies.")



if __name__ == "__main__":
    aggregate_data()
