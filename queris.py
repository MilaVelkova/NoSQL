import ast

import redis
import time
import json

redis_db = redis.Redis(host="localhost", port=6379, db=0)

def load_title(movie_key):
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

def timed_query(func, runs=10, *args, **kwargs):
    times = []
    results = None
    for _ in range(runs):
        start = time.time()
        results = func(*args, **kwargs)
        times.append(time.time() - start)
    avg_time = sum(times) / len(times)
    print(f"Average execution time over {runs} runs: {avg_time:.6f} seconds\n")
    return results

# ----------- Queries -----------

def query_by_genre(genre="Action"):
    movie_ids = redis_db.smembers(f"genre:{genre}")
    titles = [load_title(mid) for mid in movie_ids]
    print(f"[Genre: {genre}] Found {len(titles)} movies")
    return titles

def query_by_actor(actor="Tom Hanks"):
    movie_ids = redis_db.smembers(f"actor:{actor}")
    titles = [load_title(mid) for mid in movie_ids]
    print(f"[Actor: {actor}] Found {len(titles)} movies")
    return titles

def query_by_year(year="2015"):
    movie_ids = redis_db.smembers(f"year:{year}")
    titles = [load_title(mid) for mid in movie_ids]
    print(f"[Year: {year}] Found {len(titles)} movies")
    return titles

def query_by_actor_and_genre(actor="Tom Hanks", genre="Drama"):
    movie_ids = redis_db.sinter(f"actor:{actor}", f"genre:{genre}")
    titles = [load_title(mid) for mid in movie_ids]
    print(f"[Actor: {actor} AND Genre: {genre}] Found {len(titles)} movies")
    return titles

def query_by_genre_and_year(genre="Action", year="2015"):
    movie_ids = redis_db.sinter(f"genre:{genre}", f"year:{year}")
    titles = [load_title(mid) for mid in movie_ids]
    print(f"[Genre: {genre} AND Year: {year}] Found {len(titles)} movies")
    return titles

def top_rated_by_genre(genre="Drama", top_n=5):
    movie_ids = redis_db.zrevrange(f"top_rated:{genre}", 0, top_n - 1)
    titles = [load_title(mid) for mid in movie_ids]
    print(f"Top {top_n} rated movies in Genre '{genre}': {titles}")
    return titles

def top_rated_by_genre_and_year(genre="Romance", year="2019", top_n=3):
    year_ids = {mid.decode() for mid in redis_db.smembers(f"year:{year}")}
    top_ids_with_scores = redis_db.zrevrange(f"top_rated:{genre}", 0, -1, withscores=True)
    common_scored = [(mid.decode(), score) for mid, score in top_ids_with_scores if mid.decode() in year_ids]
    common_scored.sort(key=lambda x: x[1], reverse=True)
    top_common = common_scored[:top_n]
    titles = [load_title(mid) for mid, _ in top_common]
    print(f"Top {top_n} rated movies in Genre '{genre}' and Year '{year}': {titles}")
    return titles

def count_movies_by_actor(actor="Leonardo DiCaprio"):
    count = redis_db.scard(f"actor:{actor}")
    print(f"Number of movies with actor '{actor}': {count}")
    return count

def count_high_rated_action_movies(min_rating=8.0):
    ids = redis_db.zrangebyscore(f"top_rated:Action", min_rating, "+inf")
    count = len(ids)
    print(f"Number of Action movies with rating >= {min_rating}: {count}")
    return count




if __name__ == "__main__":
    print("=== Redis Queries ===")
    timed_query(query_by_genre, 20, "Action")
    timed_query(query_by_actor, 20, "Tom Hanks")
    timed_query(query_by_year, 20, "2015")
    timed_query(query_by_actor_and_genre, 20, "Tom Hanks", "Drama")
    timed_query(query_by_genre_and_year, 20, "Action", "2015")
    timed_query(top_rated_by_genre, 20, "Drama", 5)
    timed_query(top_rated_by_genre_and_year, 20, "Romance", "2019", 3)
    timed_query(count_movies_by_actor, 20, "Leonardo DiCaprio")
    timed_query(count_high_rated_action_movies, 20, 8.0)



