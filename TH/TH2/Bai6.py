from pyspark import SparkContext, SparkConf
import logging
import os
from datetime import datetime

conf = SparkConf().setAppName("Time-Based Movie Ratings Analysis").setMaster("local[*]")
sc = SparkContext(conf=conf)

log4j = sc._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

ratings1_path = "/data/ratings_1.txt"
ratings2_path = "/data/ratings_2.txt"

ratings1 = sc.textFile(ratings1_path)
ratings2 = sc.textFile(ratings2_path)
ratings = ratings1.union(ratings2)

ratings_parsed = (
    ratings.map(lambda line: line.split(','))
           .map(lambda fields: (datetime.fromtimestamp(int(fields[3])).year,
                                float(fields[2])))
)

year_stats = (
    ratings_parsed.mapValues(lambda rating: (rating, 1))
                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
)

year_avg = year_stats.mapValues(lambda v: (v[0] / v[1] if v[1] > 0 else 0, v[1]))

results = year_avg.collectAsMap()

for year, (avg, count) in sorted(results.items(), key=lambda x: x[0]):
    print(f"{year} - TotalRatings: {int(count)}, AverageRating: {avg:.2f}")

sc.stop()
