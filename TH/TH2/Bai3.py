from pyspark import SparkContext, SparkConf
import logging
import os

conf = SparkConf().setAppName("Gender-Based Movie Ratings Analysis").setMaster("local[*]")
sc = SparkContext(conf=conf)

log4j = sc._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

ratings1_path = "/data/ratings_1.txt"
ratings2_path = "/data/ratings_2.txt"
movies_path = "/data/movies.txt"
users_path = "/data/users.txt"

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
         .map(lambda fields: (int(fields[0]), fields[1]))
)

ratings_with_gender = ratings_parsed.join(users_parsed).map(
    lambda x: ((x[1][0][0], x[1][1]), x[1][0][1])
)

movie_gender_stats = ratings_with_gender.map(
    lambda x: ((x[0][0], x[0][1]), (x[1], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
)

movie_gender_avg = movie_gender_stats.mapValues(
    lambda v: v[0] / v[1] if v[1] > 0 else 0
).map(
    lambda x: (x[0][0], (x[0][1], x[1])))
  .groupByKey().mapValues(
    lambda values: (
        next((v[1] for v in values if v[0] == 'M'), 0.0),
        next((v[1] for v in values if v[0] == 'F'), 0.0)
    )
)

movies = sc.textFile(movies_path)
movies_parsed = (
    movies.map(lambda line: line.split(','))
          .map(lambda fields: (int(fields[0]), fields[1]))
)

joined = movies_parsed.join(movie_gender_avg)
results = joined.collectAsMap()

for movie_id, (title, (male_avg, female_avg)) in sorted(results.items(), key=lambda x: x[0]):
    print(f"{title} - Male_Avg: {male_avg:.2f}, Female_Avg: {female_avg:.2f}")

sc.stop()
