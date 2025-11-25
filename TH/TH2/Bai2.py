from pyspark import SparkContext, SparkConf
import logging
import os

conf = SparkConf().setAppName("Genre Ratings Analysis").setMaster("local[*]")
sc = SparkContext(conf=conf)

log4j = sc._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

ratings1_path = "/data/ratings_1.txt"
ratings2_path = "/data/ratings_2.txt"
movies_path = "/data/movies.txt"

ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)
ratings = ratings1.union(ratings2)

ratings_parsed = (
    ratings.map(lambda line: line.split(','))
           .map(lambda fields: (int(fields[1]), float(fields[2])))
)

movies = sc.textFile(movies_path)
movies_parsed = (
    movies.map(lambda line: line.split(','))
          .map(lambda fields: (int(fields[0]), fields[2].split('|')))
)

genre_ratings = ratings_parsed.join(movies_parsed).flatMap(
    lambda x: [(genre, x[1][0]) for genre in x[1][1]]
)

genre_stats = genre_ratings.mapValues(lambda rating: (rating, 1)) \
                           .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

genre_avg = genre_stats.mapValues(lambda v: (v[0] / v[1] if v[1] > 0 else 0, v[1]))

results = genre_avg.collectAsMap()

for genre, (avg, count) in sorted(results.items(), key=lambda x: x[0]):
    print(f"{genre} - AverageRating {avg:.2f} (TotalRatings {int(count)})")

sc.stop()
