from pyspark.sql import SparkSession
import logging

spark = (
    SparkSession.builder
    .appName("Order Count by Country Analysis")
    .getOrCreate()
)

log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

orders_path = "/data/Orders.csv"
customer_list_path = "/data/Customer_List.csv"

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(orders_path)
)

customer_list_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(customer_list_path)
)

orders_with_country = (
    orders_df
    .join(customer_list_df, "Customer_Trx_ID", "inner")
    .select("Order_ID", "Customer_Country")
)

order_count_by_country = (
    orders_with_country
    .groupBy("Customer_Country")
    .agg({"Order_ID": "count"})
    .withColumnRenamed("count(Order_ID)", "Order_Count")
)

order_count_sorted = order_count_by_country.orderBy(
    order_count_by_country["Order_Count"].desc()
)

order_count_sorted.show()

spark.
