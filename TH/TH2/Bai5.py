from pyspark import SparkContext, SparkConf
import logging
import os

conf = SparkConf().setAppName("Occupation-Based Movie Ratings Analysis").setMaster("local[*]")
sc = SparkContext(conf=conf)

log4j = sc._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

ratings1_path = "/data/ratings_1.txt"
ratings2_path = "/data/ratings_2.txt"
users_path = "/data/users.txt"
occupation_path = "/data/occupation.txt"

ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)
ratings = ratings1.union(ratings2)

ratings_parsed = (
    ratings.map(lambda line: line.split(','))
           .map(lambda fields: (int(fields[0]), float(fields[2])))
)

users = sc.textFile(users_path)
users_parsed = (
    users.map(lambda line: line.split(','))
         .map(lambda fields: (int(fields[0]), int(fields[3])))
)

occupations = sc.textFile(occupation_path)
occupations_parsed = (
    occupations.map(lambda line: line.split(','))
               .map(lambda fields: (int(fields[0]), fields[1]))
)

user_ratings = users_parsed.join(ratings_parsed)
occupation_ratings = user_ratings.map(
    lambda x: (x[1][0], x[1][1])
).join(occupations_parsed).map(
    lambda x: (x[1][1], x[0])
)

occupation_stats = occupation_ratings.mapValues(
    lambda rating: (rating, 1)
).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

occupation_avg = occupation_stats.mapValues(
    lambda v: (v[0] / v[1] if v[1] > 0 else 0, v[1])
)

results = occupation_avg.collectAsMap()

for occupation, (avg, count) in sorted(results.items(), key=lambda x: x[0]):
    print(f"{occupation} - TotalRatings: {int(count)}, AverageRating: {avg:.2f}")

sc.stop()
