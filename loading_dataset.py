import pandas as pd
import redis
import json
import sys

# Configuration: Number of rows to load
# You can change this value or pass as command line argument
NUM_ROWS = 5000  # Default: 5000

# Allow command line override: python loading_dataset.py 15000
if len(sys.argv) > 1:
    try:
        NUM_ROWS = int(sys.argv[1])
        print(f"Loading {NUM_ROWS} rows (from command line argument)")
    except ValueError:
        print(f"Invalid argument, using default: {NUM_ROWS} rows")
else:
    print(f"Loading {NUM_ROWS} rows (default)")

# Вчитување на податоци од CSV датотека
print(f"Reading CSV file...")
df = pd.read_csv("IMDB TMDB Movie Metadata Big Dataset (1M).csv", low_memory=False, nrows=NUM_ROWS)

# Отстранување на редови каде 'id' или 'title' се недостасуваат, бидејќи се критични за клучевите.
df = df.dropna(subset=["id", "title"])

print(f"After cleanup: {len(df)} movies to load")

# Воспоставување конекција со Redis базата на податоци
redis_db = redis.Redis(host='localhost', port=6379, db=0)

# Clear existing data (optional - comment out if you want to keep old data)
print("Clearing existing movie data from Redis...")
for key in redis_db.scan_iter("movie:*"):
    redis_db.delete(key)

# Итерирање низ DataFrame и вчитување на податоците во Redis
# Секој филм се зачувува како Key-Value пар, каде клучот е 'movie:ID', а вредноста е JSON репрезентација на филмскиот објект.
print("Loading data into Redis...")
loaded = 0
for _, row in df.iterrows():
    key = f"movie:{int(row['id'])}"
    value = json.dumps(row.dropna().to_dict())
    redis_db.set(key, value)
    loaded += 1
    if loaded % 1000 == 0:
        print(f"  Loaded {loaded} movies...")

print(f"✅ Data loaded into Redis: {loaded} movies")
print(f"\nNext step: Run 'python aggregate_movies.py' to create indexes")
