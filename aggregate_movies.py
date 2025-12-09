import redis
import json
import ast
import sys

redis_db = redis.Redis(host='localhost', port=6379, db=0)

# Configuration: Number of movies to aggregate (should match loading_dataset.py)
NUM_ROWS = 5000  # Default

# Allow command line override
if len(sys.argv) > 1:
    try:
        NUM_ROWS = int(sys.argv[1])
        print(f"Will aggregate up to {NUM_ROWS} movies (from command line)")
    except ValueError:
        print(f"Invalid argument, using default: {NUM_ROWS} movies")
else:
    print(f"Will aggregate up to {NUM_ROWS} movies (default)")

def aggregate_data():

    # CLEAN old aggregation keys to avoid type conflicts
    print("Cleaning up old aggregation keys...")
    prefixes = [
        "genre:", "actor:", "year:", "top_rated:", "director:", "country:",
        "language:", "zset:imdb_rating", "zset:vote_average", "zset:budget",
        "zset:revenue", "zset:runtime", "zset:popularity", "zset:vote_count"
    ]
    for prefix in prefixes:
        for key in redis_db.scan_iter(f"{prefix}*"):
            redis_db.delete(key)
    print("Cleanup complete.")

    # SCAN all movies
    LIMIT = NUM_ROWS  # Use the configured limit
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
                movie_key = f"movie:{movie_id}"

                # ============================================
                # Extract and parse all fields
                # ============================================

                # Ratings
                vote_avg = movie.get("vote_average")
                vote_avg = float(vote_avg) if vote_avg not in (None, "", "nan") else 0.0

                imdb_rating = movie.get("IMDB_Rating")
                imdb_rating = float(imdb_rating) if imdb_rating not in (None, "", "nan") else 0.0

                # Numeric fields
                budget = movie.get("budget")
                budget = float(budget) if budget not in (None, "", "nan") else 0.0

                revenue = movie.get("revenue")
                revenue = float(revenue) if revenue not in (None, "", "nan") else 0.0

                runtime = movie.get("runtime")
                runtime = float(runtime) if runtime not in (None, "", "nan") else 0.0

                popularity = movie.get("popularity")
                popularity = float(popularity) if popularity not in (None, "", "nan") else 0.0

                vote_count = movie.get("vote_count")
                vote_count = float(vote_count) if vote_count not in (None, "", "nan") else 0.0

                # Release year
                year_raw = movie.get("release_year")
                year = None
                try:
                    if isinstance(year_raw, (int, float)):
                        year = str(int(year_raw))
                    elif isinstance(year_raw, str) and year_raw.strip().isdigit():
                        year = str(int(year_raw.strip()))
                except:
                    year = None

                # Language
                language = movie.get("original_language", "").strip()

                # Director
                director = movie.get("director", "").strip()

                # Main actor (Star1)
                main_cast = movie.get("Star1", "").strip()

                # Parse genres list safely
                genres_raw = movie.get("genres_list", "[]")
                try:
                    genres = ast.literal_eval(genres_raw) if isinstance(genres_raw, str) else genres_raw
                    genres = [str(g).strip() for g in genres if g is not None]
                except:
                    genres = []

                # Parse production countries
                countries_raw = movie.get("production_countries", "")
                countries = []
                try:
                    if isinstance(countries_raw, str) and countries_raw:
                        # Try to parse as list
                        if countries_raw.startswith('['):
                            countries = ast.literal_eval(countries_raw)
                            countries = [str(c).strip() for c in countries if c]
                        else:
                            # Plain text, split by common delimiters
                            countries = [c.strip() for c in countries_raw.split(',')]
                except:
                    countries = []

                # ============================================
                # Create indexes and sorted sets
                # ============================================

                if movie_id:
                    # Actor index (SET)
                    if main_cast:
                        redis_db.sadd(f"actor:{main_cast}", movie_key)

                    # Director index (SET)
                    if director:
                        redis_db.sadd(f"director:{director}", movie_key)

                    # Genre indexes (SET + sorted set for ratings)
                    for genre in genres:
                        if genre:
                            redis_db.sadd(f"genre:{genre}", movie_key)
                            if vote_avg > 0:
                                redis_db.zadd(f"top_rated:{genre}", {movie_key: vote_avg})

                    # Year index (SET)
                    if year:
                        redis_db.sadd(f"year:{year}", movie_key)

                    # Language index (SET)
                    if language:
                        redis_db.sadd(f"language:{language}", movie_key)

                    # Country indexes (SET)
                    for country in countries:
                        if country:
                            redis_db.sadd(f"country:{country}", movie_key)

                    # ============================================
                    # Sorted sets for numeric fields (for range queries)
                    # ============================================

                    if imdb_rating > 0:
                        redis_db.zadd("zset:imdb_rating", {movie_key: imdb_rating})

                    if vote_avg > 0:
                        redis_db.zadd("zset:vote_average", {movie_key: vote_avg})

                    if budget > 0:
                        redis_db.zadd("zset:budget", {movie_key: budget})

                    if revenue > 0:
                        redis_db.zadd("zset:revenue", {movie_key: revenue})

                    if runtime > 0:
                        redis_db.zadd("zset:runtime", {movie_key: runtime})

                    if popularity > 0:
                        redis_db.zadd("zset:popularity", {movie_key: popularity})

                    if vote_count > 0:
                        redis_db.zadd("zset:vote_count", {movie_key: vote_count})

                processed += 1
                if processed % 100 == 0:
                    print(f"Processed {processed} movies...")

            except Exception as e:
                print(f"Error processing {key.decode() if isinstance(key, bytes) else key}: {e}")

        if cursor == 0 or processed >= LIMIT:
            break

    print(f"✅ Aggregation completed for {processed} movies.")
    print(f"\n📊 Index Summary:")
    print(f"   - Actor keys: {len(list(redis_db.scan_iter('actor:*')))}")
    print(f"   - Director keys: {len(list(redis_db.scan_iter('director:*')))}")
    print(f"   - Genre keys: {len(list(redis_db.scan_iter('genre:*')))}")
    print(f"   - Year keys: {len(list(redis_db.scan_iter('year:*')))}")
    print(f"   - Language keys: {len(list(redis_db.scan_iter('language:*')))}")
    print(f"   - Country keys: {len(list(redis_db.scan_iter('country:*')))}")
    print(f"   - Sorted sets: 7 (ratings, budget, revenue, runtime, popularity, vote_count)")



if __name__ == "__main__":
    aggregate_data()
