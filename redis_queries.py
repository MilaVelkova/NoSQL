import redis
import time
import json
import ast
import psutil
import os
from collections import defaultdict

redis_db = redis.Redis(host="localhost", port=6379, db=0)

# Get current process for CPU/memory monitoring
current_process = psutil.Process(os.getpid())

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def load_movie(movie_key):
    """Load full movie data from Redis"""
    if isinstance(movie_key, bytes):
        movie_key = movie_key.decode()
    data = redis_db.get(movie_key)
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None
    return None

def load_movies_batch(movie_keys):
    """Load multiple movies using pipeline (MUCH FASTER)"""
    if not movie_keys:
        return []
    
    # Convert bytes to strings
    keys_list = []
    for key in movie_keys:
        if isinstance(key, bytes):
            keys_list.append(key.decode())
        else:
            keys_list.append(key)
    
    # Use pipeline to batch all GET requests into one network call
    pipe = redis_db.pipeline()
    for key in keys_list:
        pipe.get(key)
    
    # Execute all at once
    raw_results = pipe.execute()
    
    # Parse results
    movies = []
    for raw in raw_results:
        if raw:
            try:
                movies.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
    
    return movies

def safe_float(value, default=0.0):
    """Safely convert to float"""
    try:
        return float(value) if value not in (None, "", "nan") else default
    except:
        return default

def safe_int(value, default=0):
    """Safely convert to int"""
    try:
        return int(value) if value not in (None, "", "nan") else default
    except:
        return default

def timed_query(func, runs=10, *args, **kwargs):
    """Run a query multiple times and calculate average execution time, CPU, and memory"""
    times = []
    cpu_percentages = []
    memory_usages = []
    results = None
    
    for _ in range(runs):
        # Reset CPU percent (first call returns 0.0, so we call it before measurement)
        current_process.cpu_percent()
        
        # Get initial memory
        mem_before = current_process.memory_info().rss / (1024 * 1024)  # MB
        
        # Run query
        start = time.time()
        results = func(*args, **kwargs)
        elapsed = time.time() - start
        times.append(elapsed)
        
        # Get CPU usage during query (interval should match query time)
        cpu_usage = current_process.cpu_percent()
        cpu_percentages.append(cpu_usage)
        
        # Get memory after query
        mem_after = current_process.memory_info().rss / (1024 * 1024)  # MB
        memory_usages.append(mem_after)
        
        # Small delay between runs
        time.sleep(0.01)
    
    avg_time = sum(times) / len(times)
    avg_cpu = sum(cpu_percentages) / len(cpu_percentages)
    avg_memory = sum(memory_usages) / len(memory_usages)
    
    print(f"Average execution time: {avg_time:.6f}s | CPU: {avg_cpu:.2f}% | Memory: {avg_memory:.2f} MB")
    
    return results, {
        "avg_time": avg_time,
        "avg_cpu": avg_cpu,
        "avg_memory": avg_memory,
        "min_time": min(times),
        "max_time": max(times)
    }


# ============================================================================
# SIMPLE QUERIES (4)
# ============================================================================

def simple_query_profitable_movies(min_budget=10000000, min_revenue_multiplier=3):
    """Simple: Profitable movies with budget filtering (multiple conditions, no JOINs)"""
    
    # Get movies with budget >= min_budget
    movie_keys = redis_db.zrevrangebyscore("zset:budget", "+inf", min_budget)
    
    # OPTIMIZED: Batch load
    movies = load_movies_batch(movie_keys)
    
    results = []
    for movie in movies:
        budget = safe_float(movie.get("budget"))
        revenue = safe_float(movie.get("revenue"))
        
        if budget > 0 and revenue > budget * min_revenue_multiplier:
            profit = revenue - budget
            roi = revenue / budget
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "budget": budget,
                "revenue": revenue,
                "profit": profit,
                "roi": roi
            })
    
    # Sort by ROI DESC
    results.sort(key=lambda x: x["roi"], reverse=True)
    
    print(f"[Simple] Profitable movies: {len(results)} results")
    return results


def simple_query_popular_recent_movies(year_start=2015, min_popularity=50, min_vote_count=1000):
    """Simple: Popular recent movies with multiple filters (no JOINs)"""

    movie_keys = redis_db.zrevrangebyscore("zset:popularity", "+inf", min_popularity)
    
    # OPTIMIZED: Batch load
    movies = load_movies_batch(movie_keys)
    
    results = []
    for movie in movies:
        year = safe_int(movie.get("release_year"))
        popularity = safe_float(movie.get("popularity"))
        vote_count = safe_float(movie.get("vote_count"))
        vote_average = safe_float(movie.get("vote_average"))
        
        if year >= year_start and vote_count >= min_vote_count and vote_average > 0:
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "release_year": year,
                "popularity": popularity,
                "vote_count": vote_count,
                "vote_average": vote_average
            })
    
    # Sort by popularity DESC
    results.sort(key=lambda x: x["popularity"], reverse=True)
    
    print(f"[Simple] Popular recent movies: {len(results)} results")
    return results


def simple_query_long_high_rated_movies(min_runtime=150, min_rating=7.5, year_start=2000):
    """Simple: Long, highly-rated movies (multiple conditions, no JOINs)"""

    movie_keys = redis_db.zrevrangebyscore("zset:runtime", "+inf", min_runtime)
    
    # OPTIMIZED: Batch load
    movies = load_movies_batch(movie_keys)
    
    results = []
    for movie in movies:
        runtime = safe_float(movie.get("runtime"))
        vote_average = safe_float(movie.get("vote_average"))
        year = safe_int(movie.get("release_year"))
        
        if vote_average >= min_rating and year >= year_start:
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "runtime": runtime,
                "vote_average": vote_average,
                "release_year": year
            })
    
    # Sort by vote_average DESC, then runtime DESC
    results.sort(key=lambda x: (x["vote_average"], x["runtime"]), reverse=True)
    
    print(f"[Simple] Long high-rated movies: {len(results)} results")
    return results


def simple_query_spanish_blockbusters(min_budget=10000000, min_revenue=20000000, language="es"):
    """Simple: Spanish language blockbusters (multiple conditions, no JOINs)"""
    
    # Get Spanish movies
    spanish_keys = redis_db.smembers(f"language:{language}")
    
    # OPTIMIZED: Batch load
    movies = load_movies_batch(spanish_keys)
    
    results = []
    for movie in movies:
        budget = safe_float(movie.get("budget"))
        revenue = safe_float(movie.get("revenue"))
        
        if budget >= min_budget and revenue >= min_revenue and budget > 0 and revenue > 0:
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "budget": budget,
                "revenue": revenue,
                "original_language": movie.get("original_language"),
                "release_year": safe_int(movie.get("release_year"))
            })
    
    # Sort by revenue DESC
    results.sort(key=lambda x: x["revenue"], reverse=True)
    
    print(f"[Simple] Spanish blockbusters: {len(results)} results")
    return results


# ============================================================================
# COMPLEX QUERIES (3)
# ============================================================================


def complex_query_multi_genre(genres=["Action", "Adventure", "Science Fiction"], min_rating=7.0):
    """Complex: Movies with multiple genres (subquery equivalent)"""
    

    high_rated_keys = redis_db.zrevrangebyscore("zset:vote_average", "+inf", min_rating)
    
    # OPTIMIZED: Batch load
    movies = load_movies_batch(high_rated_keys)
    
    results = []
    for movie in movies:
        # Parse movie genres
        genres_raw = movie.get("genres_list", "[]")
        try:
            movie_genres = ast.literal_eval(genres_raw) if isinstance(genres_raw, str) else genres_raw
            movie_genres = [str(g).strip() for g in movie_genres if g]
        except:
            movie_genres = []
        
        # Count how many of the target genres this movie has
        matching_genres = [g for g in movie_genres if g in genres]
        
        if len(matching_genres) >= 3:  #  3 genres match
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "vote_average": safe_float(movie.get("vote_average")),
                "genres": movie_genres
            })
    
    # Sort by vote_average DESC
    results.sort(key=lambda x: x["vote_average"], reverse=True)
    
    print(f"[Complex] Multi-genre movies: {len(results)} results")
    return results



def complex_query_genre_country_language(genre="Drama", country="United States of America", language="en"):
    """Complex: Movies by genre, production country, and language (4 JOINs equivalent)"""
    
    # Get movies by genre, country, and language - find intersection using Redis
    genre_keys = redis_db.smembers(f"genre:{genre}")
    country_keys = redis_db.smembers(f"country:{country}")
    language_keys = redis_db.smembers(f"language:{language}")
    
    # Find intersection of all three
    common_keys = genre_keys.intersection(country_keys).intersection(language_keys)
    
    # OPTIMIZED: Batch load
    movies = load_movies_batch(common_keys)
    
    results = []
    for movie in movies:
        year = safe_int(movie.get("release_year"))
        vote_average = safe_float(movie.get("vote_average"))
        
        if year >= 2010 and vote_average > 0:
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "release_year": year,
                "vote_average": vote_average
            })
    
    # Sort by vote_average DESC
    results.sort(key=lambda x: x["vote_average"], reverse=True)
    
    print(f"[Complex] Genre+Country+Language: {len(results)} results")
    return results


def complex_query_high_budget_profit(min_budget=50000000):
    """Redis: Movies with high budget, profit, and genres (like Postgres)"""

    # Get all movies with budget >= min_budget
    movie_keys = redis_db.zrevrangebyscore("zset:budget", "+inf", min_budget)

    # Batch load
    movies = load_movies_batch(movie_keys)

    results = []
    for movie in movies:
        budget = safe_float(movie.get("budget"))
        revenue = safe_float(movie.get("revenue"))

        if budget > 0 and revenue > budget:
            # Aggregate genres
            genres_raw = movie.get("genres_list", "[]")
            try:
                movie_genres = ast.literal_eval(genres_raw) if isinstance(genres_raw, str) else genres_raw
                movie_genres = list({str(g).strip() for g in movie_genres if g})  # unique genres
            except:
                movie_genres = []

            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "budget": budget,
                "revenue": revenue,
                "profit": revenue - budget,
                "genres": movie_genres
            })

    # Sort by profit DESC (like Postgres)
    results.sort(key=lambda x: x["profit"], reverse=True)

    print(f"[Complex] High budget profit: {len(results)} results")
    return results


# ============================================================================
# AGGREGATED QUERIES (5)
# ============================================================================

def aggregate_movies_per_year():
    """Aggregate: Count movies per year"""
    
    # Get all year keys
    year_keys = list(redis_db.scan_iter("year:*"))
    
    results = []
    for key in year_keys:
        year = key.decode().split("year:")[1] if isinstance(key, bytes) else key.split("year:")[1]
        count = redis_db.scard(key)
        
        if year:
            results.append({
                "release_year": int(year),
                "movie_count": count
            })
    
    # Sort by year DESC
    results.sort(key=lambda x: x["release_year"], reverse=True)
    
    print(f"[Aggregate] Movies per year: {len(results)} years")
    return results


def aggregate_avg_rating_per_genre():
    """Aggregate: Average rating per genre"""
    
    # Get all genre keys
    genre_keys = list(redis_db.scan_iter("genre:*"))
    
    results = []
    for key in genre_keys:
        genre_name = key.decode().split("genre:")[1] if isinstance(key, bytes) else key.split("genre:")[1]
        
        if genre_name.startswith("top_rated"):  # Skip top_rated keys
            continue
        
        movie_keys = redis_db.smembers(key)
        
        # OPTIMIZED: Batch load all movies for this genre
        movies = load_movies_batch(movie_keys)
        
        ratings = []
        for movie in movies:
            vote_avg = safe_float(movie.get("vote_average"))
            if vote_avg > 0:
                ratings.append(vote_avg)
        
        if ratings:
            results.append({
                "name": genre_name,
                "movie_count": len(ratings),
                "avg_rating": sum(ratings) / len(ratings),
                "max_rating": max(ratings),
                "min_rating": min(ratings)
            })
    
    # Sort by avg_rating DESC
    results.sort(key=lambda x: x["avg_rating"], reverse=True)
    
    print(f"[Aggregate] Average rating per genre: {len(results)} genres")
    return results


def aggregate_top_actors_by_movie_count(top_n=10):
    """Aggregate: Most prolific actors"""
    
    # Get all actor keys
    actor_keys = list(redis_db.scan_iter("actor:*"))
    
    results = []
    for key in actor_keys:
        actor_name = key.decode().split("actor:")[1] if isinstance(key, bytes) else key.split("actor:")[1]
        
        movie_keys = redis_db.smembers(key)
        movie_count = len(movie_keys)
        
        if movie_count >= 3:  # HAVING clause equivalent
            # OPTIMIZED: Batch load all movies for this actor
            movies = load_movies_batch(movie_keys)
            
            ratings = []
            for movie in movies:
                vote_avg = safe_float(movie.get("vote_average"))
                if vote_avg > 0:
                    ratings.append(vote_avg)
            
            if ratings:
                avg_rating = sum(ratings) / len(ratings)
                results.append({
                    "name": actor_name,
                    "movie_count": movie_count,
                    "avg_movie_rating": avg_rating
                })
    
    # Sort by movie_count DESC, then avg_rating DESC
    results.sort(key=lambda x: (x["movie_count"], x["avg_movie_rating"]), reverse=True)
    results = results[:top_n]
    
    print(f"[Aggregate] Top actors: {len(results)} actors")
    return results


def aggregate_yearly_trends():
    """Aggregate: Yearly movie industry trends"""
    
    # Get all year keys
    year_keys = list(redis_db.scan_iter("year:*"))
    
    results = []
    for key in year_keys:
        year = key.decode().split("year:")[1] if isinstance(key, bytes) else key.split("year:")[1]
        
        try:
            year_int = int(year)
        except:
            continue
            
        if year_int < 1990:
            continue
        
        movie_keys = redis_db.smembers(key)
        
        # OPTIMIZED: Batch load all movies for this year
        movies = load_movies_batch(movie_keys)
        
        ratings = []
        budgets = []
        revenues = []
        runtimes = []
        high_rated_count = 0
        
        for movie in movies:
            vote_avg = safe_float(movie.get("vote_average"))
            budget = safe_float(movie.get("budget"))
            revenue = safe_float(movie.get("revenue"))
            runtime = safe_float(movie.get("runtime"))
            
            if vote_avg > 0:
                ratings.append(vote_avg)
                if vote_avg >= 7.0:
                    high_rated_count += 1
            
            if budget > 0:
                budgets.append(budget)
                
            if revenue > 0:
                revenues.append(revenue)
                
            if runtime > 0:
                runtimes.append(runtime)
        
        results.append({
            "release_year": year_int,
            "movie_count": len(movie_keys),
            "avg_rating": sum(ratings) / len(ratings) if ratings else 0,
            "avg_budget": sum(budgets) / len(budgets) if budgets else 0,
            "avg_revenue": sum(revenues) / len(revenues) if revenues else 0,
            "avg_runtime": sum(runtimes) / len(runtimes) if runtimes else 0,
            "high_rated_count": high_rated_count
        })
    
    # Sort by year DESC
    results.sort(key=lambda x: x["release_year"], reverse=True)
    
    print(f"[Aggregate] Yearly trends: {len(results)} years")
    return results


def aggregate_genre_combinations():
    """Aggregate: Most common genre combinations"""
    
    # Get all movies and group by genre combinations
    genre_combos = defaultdict(lambda: {"count": 0, "ratings": []})
    
    # OPTIMIZED: Get all movie keys and batch load them
    all_movie_keys = list(redis_db.scan_iter("movie:*"))
    
    # Process in batches of 500 to avoid memory issues
    batch_size = 500
    for i in range(0, len(all_movie_keys), batch_size):
        batch_keys = all_movie_keys[i:i+batch_size]
        movies = load_movies_batch(batch_keys)
        
        for movie in movies:
            # Parse genres
            genres_raw = movie.get("genres_list", "[]")
            try:
                genres = ast.literal_eval(genres_raw) if isinstance(genres_raw, str) else genres_raw
                genres = [str(g).strip() for g in genres if g]
            except:
                genres = []
            
            # Only consider movies with multiple genres
            if len(genres) > 1:
                genres_key = tuple(sorted(genres))
                genre_combos[genres_key]["count"] += 1
                
                vote_avg = safe_float(movie.get("vote_average"))
                if vote_avg > 0:
                    genre_combos[genres_key]["ratings"].append(vote_avg)
    
    # Convert to results list
    results = []
    for genres_tuple, data in genre_combos.items():
        if data["count"] >= 3:  # HAVING clause equivalent
            avg_rating = sum(data["ratings"]) / len(data["ratings"]) if data["ratings"] else 0
            results.append({
                "genres": list(genres_tuple),
                "movie_count": data["count"],
                "avg_rating": avg_rating
            })
    
    # Sort by movie_count DESC
    results.sort(key=lambda x: x["movie_count"], reverse=True)
    
    print(f"[Aggregate] Genre combinations: {len(results)} combinations")
    return results


# ============================================================================
# MAIN - Run all queries with timing
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("REDIS QUERY BENCHMARKS")
    print("=" * 80)
    
    # Get Redis memory info before queries
    redis_info = redis_db.info('memory')
    print(f"\nRedis Memory Before Queries: {redis_info['used_memory_human']}")
    print(f"Total Keys in DB: {redis_db.dbsize():,}")
    
    # Store results for summary
    results = {}
    
    print("\n" + "=" * 80)
    print("SIMPLE QUERIES (5)")
    print("=" * 80)
    
    # _, metrics = timed_query(simple_query_recent_high_rated, 10, 2010, 2020, 8.0)
    # results["Simple 1: Recent high-rated"] = metrics
    
    _, metrics = timed_query(simple_query_profitable_movies, 10, 10000000, 3)
    results["Simple 2: Profitable movies"] = metrics
    
    _, metrics = timed_query(simple_query_popular_recent_movies, 10, 2015, 50, 1000)
    results["Simple 3: Popular recent"] = metrics
    
    _, metrics = timed_query(simple_query_long_high_rated_movies, 10, 150, 7.5, 2000)
    results["Simple 4: Long high-rated"] = metrics
    
    _, metrics = timed_query(simple_query_spanish_blockbusters, 10, 10000000, 20000000, "es")
    results["Simple 5: Spanish blockbusters"] = metrics
    
    print("\n" + "=" * 80)
    print("COMPLEX QUERIES (5)")
    print("=" * 80)

    
    _, metrics = timed_query(complex_query_multi_genre, 10, ["Action", "Adventure", "Science Fiction"], 7.0)
    results["Complex 2: Multi-genre"] = metrics

    
    _, metrics = timed_query(complex_query_genre_country_language, 10, "Drama", "United States of America", "en")
    results["Complex 4: Genre+Country+Lang"] = metrics
    
    _, metrics = timed_query(complex_query_high_budget_profit, 10, 50000000)
    results["Complex 5: High budget profit"] = metrics
    
    print("\n" + "=" * 80)
    print("AGGREGATED QUERIES (5)")
    print("=" * 80)
    
    _, metrics = timed_query(aggregate_movies_per_year, 10)
    results["Aggregate 1: Movies per year"] = metrics
    
    _, metrics = timed_query(aggregate_avg_rating_per_genre, 10)
    results["Aggregate 2: Avg rating per genre"] = metrics
    
    _, metrics = timed_query(aggregate_top_actors_by_movie_count, 10, 10)
    results["Aggregate 3: Top actors"] = metrics
    
    _, metrics = timed_query(aggregate_yearly_trends, 10)
    results["Aggregate 4: Yearly trends"] = metrics
    
    _, metrics = timed_query(aggregate_genre_combinations, 10)
    results["Aggregate 5: Genre combinations"] = metrics
    
    # Calculate category averages
    simple_results = [v for k, v in results.items() if "Simple" in k]
    complex_results = [v for k, v in results.items() if "Complex" in k]
    aggregate_results = [v for k, v in results.items() if "Aggregate" in k]
    
    simple_avg_time = sum([r["avg_time"] for r in simple_results]) / len(simple_results)
    simple_avg_cpu = sum([r["avg_cpu"] for r in simple_results]) / len(simple_results)
    simple_avg_memory = sum([r["avg_memory"] for r in simple_results]) / len(simple_results)
    
    complex_avg_time = sum([r["avg_time"] for r in complex_results]) / len(complex_results)
    complex_avg_cpu = sum([r["avg_cpu"] for r in complex_results]) / len(complex_results)
    complex_avg_memory = sum([r["avg_memory"] for r in complex_results]) / len(complex_results)
    
    aggregate_avg_time = sum([r["avg_time"] for r in aggregate_results]) / len(aggregate_results)
    aggregate_avg_cpu = sum([r["avg_cpu"] for r in aggregate_results]) / len(aggregate_results)
    aggregate_avg_memory = sum([r["avg_memory"] for r in aggregate_results]) / len(aggregate_results)
    
    overall_avg_time = sum([r["avg_time"] for r in results.values()]) / len(results)
    overall_avg_cpu = sum([r["avg_cpu"] for r in results.values()]) / len(results)
    overall_avg_memory = sum([r["avg_memory"] for r in results.values()]) / len(results)
    
    # Print summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY BY CATEGORY")
    print("=" * 80)
    
    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ SIMPLE QUERIES (4)                                                      │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {simple_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {simple_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {simple_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")
    
    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ COMPLEX QUERIES (3)                                                     │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {complex_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {complex_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {complex_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")
    
    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ AGGREGATED QUERIES (5)                                                  │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {aggregate_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {aggregate_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {aggregate_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")
    
    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ OVERALL (12 QUERIES)                                                    │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {overall_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {overall_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {overall_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")
    
    print("\n" + "=" * 80)
    print("DETAILED RESULTS (All 15 Queries)")
    print("=" * 80)
    print(f"\n{'Query Name':<40} {'Time (s)':<12} {'CPU (%)':<10} {'Memory (MB)':<12}")
    print("-" * 80)
    for query_name, metrics in results.items():
        print(f"{query_name:<40} {metrics['avg_time']:<12.6f} {metrics['avg_cpu']:<10.2f} {metrics['avg_memory']:<12.2f}")
    
    # Get Redis memory info after queries
    redis_info_after = redis_db.info('memory')
    print("\n" + "=" * 80)
    print("REDIS MEMORY INFO")
    print("=" * 80)
    print(f"Used Memory:      {redis_info_after['used_memory_human']}")
    print(f"Peak Memory:      {redis_info_after['used_memory_peak_human']}")
    print(f"Total Keys:       {redis_db.dbsize():,}")
    
    # Save results to JSON
    output_data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "database": "Redis",
        "total_keys": redis_db.dbsize(),
        "redis_memory": redis_info_after['used_memory_human'],
        "category_averages": {
            "simple": {
                "avg_time": simple_avg_time,
                "avg_cpu": simple_avg_cpu,
                "avg_memory": simple_avg_memory
            },
            "complex": {
                "avg_time": complex_avg_time,
                "avg_cpu": complex_avg_cpu,
                "avg_memory": complex_avg_memory
            },
            "aggregated": {
                "avg_time": aggregate_avg_time,
                "avg_cpu": aggregate_avg_cpu,
                "avg_memory": aggregate_avg_memory
            },
            "overall": {
                "avg_time": overall_avg_time,
                "avg_cpu": overall_avg_cpu,
                "avg_memory": overall_avg_memory
            }
        },
        "detailed_results": results
    }
    
    output_file = f"redis_benchmark_results_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n✅ Results saved to: {output_file}")
    print("=" * 80)

