# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

# Creation of Spark Session object and initial Dataframe
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("Json").option("inferSchema", "true").load("/FileStore/tables/TakeAway/data.json")

#Transformations to explode and flatten the nested structs 
df1 = df.withColumn("orders", F.explode("orders")).select("customerId", F.col("orders.orderId").alias("orderId"), F.col("orders.basket").alias("basket")).drop("orders")
df2 = df1.withColumn("basket", F.explode("basket").alias("basket")).select("customerId", "orderId", F.col("basket.productId").alias("productId"), F.col("basket.productType").alias("productType"), F.col("basket.grossMerchandiseValueEur").alias("grossMerchandiseValueEur"))

#UDF to calculate the Net Merchandise values of an order
def netMerchCal(x, y):
  if x == "beverage":
    return y+0.09*y
  elif x == "hot food":
    return y+0.15*y
  else:
    return y+0.07*y
 
#Regester the UDF 
netMerch_udf = F.udf(lambda a, b : netMerchCal(a, b), DoubleType())

#Adding a new column to store the Net Merchandise value of a product
df3 = df2.withColumn("netMerch", netMerch_udf("productType", "grossMerchandiseValueEur"))

#Aggregating the Net merchandise values of all products for an orderID 
df3.groupBy("orderId").agg(F.sum("netMerch")).write.parquet("/output/result.parquet")
