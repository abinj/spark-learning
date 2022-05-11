import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practice').getOrCreate()
print(spark)


df_pyspark = spark.read.option("header", "true").csv("../dataset/flights.csv")
print(df_pyspark.show())

print(type(df_pyspark))

print(df_pyspark.head(5))
print(df_pyspark.printSchema())

