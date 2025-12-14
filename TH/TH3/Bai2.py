from pyspark.sql import SparkSession
import logging

spark = (
    SparkSession.builder
    .appName("Order Statistics Analysis")
    .getOrCreate()
)

log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

orders_path = "/data/Orders.csv"
customer_list_path = "/data/Customer_List.csv"
order_items_path = "/data/Order_Items.csv"

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

order_items_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(order_items_path)
)

total_orders = orders_df.select("Order_ID").distinct().count()
total_customers = customer_list_df.select("Subscriber_ID").distinct().count()
total_sellers = order_items_df.select("Seller_ID").distinct().count()

print(f"Total Orders: {total_orders}")
print(f"Total Customers: {total_customers}")
print(f"Total Sellers: {total_sellers}")

spark.stop()
