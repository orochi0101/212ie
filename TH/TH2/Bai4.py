from pyspark import SparkContext, SparkConf
import logging
import os

conf = SparkConf().setAppName("Age-Based Movie Ratings Analysis").setMaster("local[*]")
sc = SparkContext(conf=conf)

log4j = sc._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

ratings1_path = "/data/ratings_1.txt"
ratings2_path = "/data/ratings_2.txt"
movies_path = "/data/movies.txt"
users_path = "/data/users.txt"

def get_age_group(age):
    if age <= 18:
        return "0-18"
    elif age <= 35:
        return "18-35"
    elif age <= 50:
        return "35-50"
    else:
        return "50+"

ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)
ratings = ratings1.union(ratings2)

ratings_parsed = (
    ratings.map(lambda line: line.split(','))
           .map(lambda fields: (int(fields[0]), (int(fields[1]), float(fields[2]))))
)

users = sc.textFile(users_path)
users_parsed = (
    users.map(lambda line: line.split(','))
         .map(lambda fields: (int(fields[0]), get_age_group(int(fields[2]))))
)

ratings_with_age = ratings_parsed.join(users_parsed).map(
    lambda x: ((x[1][0][0], x[1][1]), x[1][0][1])
)

movie_age_stats = ratings_with_age.map(
    lambda x: ((x[0][0], x[0][1]), (x[1], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
)

movie_age_avg = movie_age_stats.mapValues(
    lambda v: v[0] / v[1] if v[1] > 0 else None
).map(
    lambda x: (x[0][0], {x[0][1]: x[1]}))
  .reduceByKey(lambda a, b: {**a, **b}) \
  .mapValues(
    lambda v: {
        "0-18": v.get("0-18", "NA"),
        "18-35": v.get("18-35", "NA"),
        "35-50": v.get("35-50", "NA"),
        "50+": v.get("50+", "NA")
    }
)

movies = sc.textFile(movies_path)
movies_parsed = (
    movies.map(lambda line: line.split(','))
          .map(lambda fields: (int(fields[0]), fields[1]))
)

joined = movies_parsed.join(movie_age_avg)

results = joined.collectAsMap()

for movie_id, (title, age_ratings) in sorted(results.items(), key=lambda x: x[0]):
    formatted_ratings = [
        f"0-18: {age_ratings['0-18']:.2f}" if age_ratings['0-18'] != "NA" else "0-18: NA",
        f"18-35: {age_ratings['18-35']:.2f}" if age_ratings['18-35'] != "NA" else "18-35: NA",
        f"35-50: {age_ratings['35-50']:.2f}" if age_ratings['35-50'] != "NA" else "35-50: NA",
        f"50+: {age_ratings['50+']:.2f}" if age_ratings['50+'] != "NA" else "50+: NA"
    ]
    print(f"{title} - [{', '.join(formatted_ratings)}]")

sc.stop()
