import redis
import time
import json

redis_db = redis.Redis(host="localhost", port=6379, db=0)
def load_title(movie_key):
    """Load movie title by Redis key like 'movie:336197'."""
    if isinstance(movie_key, bytes):
        movie_key = movie_key.decode()
    data = redis_db.get(movie_key)
    if data:
        try:
            movie = json.loads(data)
            return movie.get("title", "Unknown Title")
        except json.JSONDecodeError:
            return "Invalid JSON"
    return "Not Found"

# ----------- Simple Queries -----------

def query_by_genre(genre="Action"):
    start = time.time()
    movie_ids = redis_db.smembers(f"genre:{genre}")
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Genre: {genre}] Found {len(titles)} movies in {end - start:.4f} seconds")
    # Uncomment below to print titles
    # print(titles)

def query_by_actor(actor="Tom Hanks"):
    start = time.time()
    movie_ids = redis_db.smembers(f"actor:{actor}")
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Actor: {actor}] Found {len(titles)} movies in {end - start:.4f} seconds")

def query_by_year(year="2015"):
    start = time.time()
    movie_ids = redis_db.smembers(f"year:{year}")
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Year: {year}] Found {len(titles)} movies in {end - start:.4f} seconds")

# ----------- Complex Queries -----------

def query_by_actor_and_genre(actor="Tom Hanks", genre="Drama"):
    start = time.time()
    movie_ids = redis_db.sinter(f"actor:{actor}", f"genre:{genre}")
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Actor: {actor} AND Genre: {genre}] Found {len(titles)} movies in {end - start:.4f} seconds")

def query_by_genre_and_year(genre="Action", year="2015"):
    start = time.time()
    movie_ids = redis_db.sinter(f"genre:{genre}", f"year:{year}")
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Genre: {genre} AND Year: {year}] Found {len(titles)} movies in {end - start:.4f} seconds")

# def query_by_genre_and_actor(genre="Adventure", actor="Matthew McConaughey"):
#     start = time.time()
#     movie_ids = redis_db.sinter(f"genre:{genre}", f"actor:{actor}")
#     titles = [load_title(mid) for mid in movie_ids]
#     end = time.time()
#     print(f"[Genre: {genre} AND Actor: {actor}] Found {len(titles)} movies in {end - start:.4f} seconds")

# ----------- Aggregated Queries -----------

def top_rated_by_genre(genre="Drama", top_n=5):
    start = time.time()
    movie_ids = redis_db.zrevrange(f"top_rated:{genre}", 0, top_n - 1)
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"Top {top_n} rated movies in Genre '{genre}':")
    for i, title in enumerate(titles, 1):
        print(f"{i}. {title}")
    print(f"Query time: {end - start:.4f} seconds\n")

def top_rated_by_genre_and_year(genre="Romance", year="2019", top_n=3):
    start = time.time()
    year_ids_raw = redis_db.smembers(f"year:{year}")
    year_ids = {mid.decode() for mid in year_ids_raw}
    top_ids_with_scores = redis_db.zrevrange(f"top_rated:{genre}", 0, -1, withscores=True)

    common_scored = []
    for mid_bytes, score in top_ids_with_scores:
        mid_str = mid_bytes.decode()
        if mid_str in year_ids:
            common_scored.append((mid_str, score))

    common_scored.sort(key=lambda x: x[1], reverse=True)

    top_common = common_scored[:top_n]
    titles = [load_title(mid) for mid, _ in top_common]
    end = time.time()

    print(f"Top {top_n} rated movies in Genre '{genre}' and Year '{year}':")
    if not titles:
        print("No movies found for this combination.")
    for i, title in enumerate(titles, 1):
        print(f"{i}. {title}")
    print(f"Query time: {end - start:.4f} seconds\n")

# ----------- Utility Queries -----------

def count_movies_by_actor(actor="Leonardo DiCaprio"):
    start = time.time()
    count = redis_db.scard(f"actor:{actor}")
    end = time.time()
    print(f"Number of movies with actor '{actor}': {count} (queried in {end - start:.4f} seconds)")

def count_high_rated_action_movies(min_rating=8.0):
    start = time.time()
    ids = redis_db.zrangebyscore(f"top_rated:Action", min_rating, "+inf")
    count = len(ids)
    end = time.time()
    print(f"Number of Action movies with rating >= {min_rating}: {count} (queried in {end - start:.4f} seconds)")

# ----------- Main -----------

if __name__ == "__main__":
    print("=== Simple Queries ===")
    query_by_genre("Action")
    query_by_actor("Tom Hanks")
    query_by_year("2015")

    print("\n=== Complex Queries ===")
    query_by_actor_and_genre("Tom Hanks", "Drama")
    query_by_genre_and_year("Action", "2015")
    # query_by_genre_and_actor("Adventure", "Matthew McConaughey")

    print("\n=== Aggregated Queries ===")
    top_rated_by_genre("Drama", top_n=5)
    top_rated_by_genre_and_year("Romance", "2019", top_n=3)

    print("\n=== Utility Queries ===")
    count_movies_by_actor("Leonardo DiCaprio")
    count_high_rated_action_movies(8.0)
