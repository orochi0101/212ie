from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import year, month

spark = (
    SparkSession.builder
    .appName("Order Count by Year and Month Analysis")
    .getOrCreate()
)

log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

orders_path = "/data/Orders.csv"

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(orders_path)
)

orders_with_date = (
    orders_df
    .withColumn("Year", year("Order_Purchase_Timestamp"))
    .withColumn("Month", month("Order_Purchase_Timestamp"))
)

order_count_by_year_month = (
    orders_with_date
    .groupBy("Year", "Month")
    .agg({"Order_ID": "count"})
    .withColumnRenamed("count(Order_ID)", "Order_Count")
)

order_count_sorted = order_count_by_year_month.orderBy(
    "Year",
    order_count_by_year_month["Month"].desc()
)

order_count_sorted.show()

spark.stop()
