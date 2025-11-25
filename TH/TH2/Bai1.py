from pyspark import SparkContext, SparkConf
import logging
import os

conf = SparkConf().setAppName("Movie Ratings Analysis").setMaster("local[*]")
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

movie_stats = ratings_parsed.aggregateByKey(
    (0.0, 0),
    lambda acc, val: (acc[0] + val, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)

movie_avg = movie_stats.mapValues(
    lambda v: (v[0] / v[1] if v[1] > 0 else 0, v[1])
)

movies = sc.textFile(movies_path)
movies_parsed = (
    movies.map(lambda line: line.split(','))
          .map(lambda fields: (int(fields[0]), fields[1]))
)

joined = movies_parsed.join(movie_avg)
results = joined.collectAsMap()

for movie_id, (title, (avg, count)) in sorted(results.items(), key=lambda x: x[0]):
    print(f"{title} AverageRating: {avg:.2f} (TotalRatings: {int(count)})")

filtered = {k: v for k, v in results.items() if v[1][1] >= 5}

if filtered:
    highest = max(filtered.items(), key=lambda x: x[1][1][0])
    highest_title = highest[1][0]
    highest_avg = highest[1][1][0]
    print(
        f"{highest_title} is the highest rated movie with an average "
        f"rating of {highest_avg:.2f} among movies with at least 5 ratings."
    )

sc.stop()
