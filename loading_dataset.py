import pandas as pd
import redis
import json

# Вчитување на податоци од CSV датотека
# Поради големината на оригиналното податочно множество (1M записи), за целите на тестирање и развој, се користат првите 5000 записи.
df = pd.read_csv("IMDB TMDB Movie Metadata Big Dataset (1M).csv", low_memory=False, nrows=5000)

# Отстранување на редови каде 'id' или 'title' се недостасуваат, бидејќи се критични за клучевите.
df = df.dropna(subset=["id", "title"])

# Воспоставување конекција со Redis базата на податоци
redis_db = redis.Redis(host='localhost', port=6379, db=0)

# Итерирање низ DataFrame и вчитување на податоците во Redis
# Секој филм се зачувува како Key-Value пар, каде клучот е 'movie:ID', а вредноста е JSON репрезентација на филмскиот објект.
for _, row in df.iterrows():
    key = f"movie:{int(row['id'])}"
    value = json.dumps(row.dropna().to_dict())
    redis_db.set(key, value)


print("✅ Data loaded into Redis.")
