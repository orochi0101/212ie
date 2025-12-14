from pyspark.sql import SparkSession
import logging

spark = (
    SparkSession.builder
    .appName("Read CSV Data with Custom Delimiter")
    .getOrCreate()
)

log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

customer_list_path = "/data/Customer_List.csv"
order_items_path = "/data/Order_Items.csv"
order_reviews_path = "/data/Order_Reviews.csv"
orders_path = "/data/Orders.csv"
products_path = "/data/Products.csv"

customer_list_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(customer_list_path)
)

order_items_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(order_items_path)
)

order_reviews_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(order_reviews_path)
)

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(orders_path)
)

products_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(products_path)
)

print("Customer_List Schema:")
customer_list_df.printSchema()

print("\nOrder_Items Schema:")
order_items_df.printSchema()

print("\nOrder_Reviews Schema:")
order_reviews_df.printSchema()

print("\nOrders Schema:")
orders_df.printSchema()

print("\nProducts Schema:")
products_df.printSchema()

spark.stop()
