from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("part1").getOrCreate()

# Read the dataset

# df_pyspark = spark.read.option('header', 'true').csv("../dataset/flights.csv", inferSchema=True)
#
# df_pyspark.printSchema()

df_pyspark = spark.read.csv("../dataset/flights.csv", header=True, inferSchema=True)
df_pyspark.printSchema()

print(df_pyspark.columns)

df_pyspark.select(['mon', 'dom']).show()

print(df_pyspark.dtypes)

df_pyspark.describe().show()

## Adding columns in dataframe
df_pyspark.withColumn("mon after 2 years", df_pyspark["mon"] + 2).show()

## Drop the columns
df_pyspark.drop('mon after 2 years').show()

# Rename the columns
df_pyspark.withColumnRenamed('mon', 'new_mon').show()
