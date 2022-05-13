from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("part_3").getOrCreate()

df_pyspark = spark.read.csv("../dataset/flights.csv", header=True, inferSchema=True)

df_pyspark.filter((df_pyspark['mon'] <= 20) &
                  (df_pyspark['dom'] <= 5)).show()

# Passing condition and column name internally using string and select only selected columns
df_pyspark.filter("mon<=20").select(["mon", "dom"]).show()

# column values not less than or equal to given value
df_pyspark.filter(~(df_pyspark['mon'] < 5)).show()