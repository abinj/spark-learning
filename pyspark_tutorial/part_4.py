## GroupBy and Aggregate functions

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("part_4").getOrCreate()

# df_pyspark = spark.read.csv("../dataset/test3.csv", header=True, inferSchema=True)
# df_pyspark.printSchema()
#
# df_pyspark.groupBy('Name').sum().show()
#
#
# df_pyspark.groupBy("Departments").mean().show()
#
# df_pyspark.groupBy("Departments").max().show()
#
# df_pyspark.groupBy("Departments").min().show()
#
# df_pyspark.groupBy("Name").avg().show()




rdd = spark.sparkContext.parallelize((0,1,2,3,4,5,6,7))
print(rdd.collect())
print(rdd.getNumPartitions())
rdd1 = rdd.coalesce(2)
print(rdd1.getNumPartitions())

