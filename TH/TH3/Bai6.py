from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import year, col

spark = (
    SparkSession.builder
    .appName("Revenue by Product Category in 2024")
    .getOrCreate()
)

log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)

orders_path = "/data/Orders.csv"
order_items_path = "/data/Order_Items.csv"
products_path = "/data/Products.csv"

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(orders_path)
)

order_items_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(order_items_path)
)

products_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .csv(products_path)
)

order_items_with_orders = (
    order_items_df
    .join(orders_df, "Order_ID", "inner")
    .select(
        "Order_ID",
        "Order_Item_ID",
        "Product_ID",
        "Seller_ID",
        "Price",
        "Freight_Value",
        "Order_Purchase_Timestamp"
    )
)

order_data = (
    order_items_with_orders
    .join(products_df, "Product_ID", "inner")
    .select(
        "Order_ID",
        "Product_Category_Name",
        "Price",
        "Freight_Value",
        "Order_Purchase_Timestamp"
    )
)

revenue_df = (
    order_data
    .withColumn("Year", year("Order_Purchase_Timestamp"))
    .withColumn("Revenue", col("Price") + col("Freight_Value"))
)

revenue_2024_df = revenue_df.filter(col("Year") == 2024)

category_revenue = (
    revenue_2024_df
    .groupBy("Product_Category_Name")
    .agg({"Revenue": "sum"})
    .withColumnRenamed("sum(Revenue)", "Total_Revenue")
)

category_revenue_sorted = category_revenue.orderBy(
    col("Total_Revenue").desc()
)

category_revenue_sorted.show()

spark.stop()
