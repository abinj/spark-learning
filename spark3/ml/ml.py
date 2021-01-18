from pyspark import SQLContext, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark= SparkSession.builder.appName("ml").getOrCreate()


# CSV 2 Parquet
schema = StructType([
    StructField("mon", IntegerType(), True),
    StructField("dom", IntegerType(), True),
    StructField("dow", IntegerType(), True),
    StructField("carrier", StringType(), True),
    StructField("flight", IntegerType(), True),
    StructField("org", StringType(), True),
    StructField("mile", IntegerType(), True),
    StructField("depart", DoubleType(), True),
    StructField("duration", IntegerType(), True),
    StructField("delay", IntegerType(), True)])

df = spark.read.csv("/home/abin/my_works/github_works/spark-learning/dataset/flights.csv", header=True, sep=",")
df.write.parquet('/home/abin/my_works/github_works/spark-learning/dataset/flights-parquet.parquet')


# Parquet 2 CSV
readdf = spark.read.parquet('/home/abin/my_works/github_works/spark-learning/dataset/flights-parquet.parquet')
readdf.rdd.map(tuple).map(lambda row: str(row[0]) + "," + str(row[1]) + ","+ str(row[2]) + ","+ str(row[3])+ ","+
                              str(row[4])+","+ str(row[5]) + ","+ str(row[6]) + ","+ str(row[7]) + ","+ str(row[8]) + ","+ str(row[9]))\
    .saveAsTextFile("/home/abin/my_works/github_works/spark-learning/dataset/flights_new.csv")

