from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import col, mean, count, regexp_extract, when

spark = (
    SparkSession.builder
    .appName("Review Score Summary Analysis")
    .getOrCreate()
)

log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

order_reviews_path = "/data/Order_Reviews.csv"

order_reviews_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(order_reviews_path)
)

print("Schema of Order_Reviews:")
order_reviews_df.printSchema()

valid_reviews_df = (
    order_reviews_df
    .withColumn(
        "Review_Score_Cast",
        when(
            regexp_extract(col("Review_Score"), r"^[1-5](\.0+)?$", 0) != "",
            col("Review_Score").cast("int")
        )
    )
    .filter(col("Review_Score_Cast").isNotNull())
    .select("Review_Score_Cast", "Review_ID")
)

overall_avg = (
    valid_reviews_df
    .agg(mean("Review_Score_Cast").alias("Overall_Average_Score"))
    .collect()[0]["Overall_Average_Score"]
)

review_counts = (
    valid_reviews_df
    .groupBy("Review_Score_Cast")
    .agg(count("Review_Score_Cast").alias("Review_Count"))
    .orderBy("Review_Score_Cast")
)

counts = review_counts.collect()
score_counts = {row["Review_Score_Cast"]: row["Review_Count"] for row in counts}

print(f"Overall Average Score: {overall_avg:.2f}")
for score in range(1, 6):
    count = score_counts.get(score, 0)
    print(f"Score {score}: {count} reviews")

spark.stop()
