from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("read_write_data") \
    .getOrCreate()

print(spark.sparkContext.getConf().getAll())
# path = "/home/abin/my_works/github_works/spark-learning/dataset/sparkify_log_small.json"
user_log = spark.read.csv("/home/abin/my_works/sightline_innovation/task_173/data/data_2016_cleaned.csv", header=True)

# user_log.printSchema()
print(user_log.count())

# user_log.describe()
#
# user_log.show(n=1)
#
# user_log.take(5)
#
# out_path = "/home/abin/my_works/github_works/spark-learning/dataset/sparkify_log_small.csv"
#
# user_log.write.save(out_path, format='csv', header=True)
# user_log_2 = spark.read.csv(out_path, header=True)
#
# user_log_2.printSchema
#
# user_log_2.take(2)
#
# user_log_2.select("userID").show()
#
# user_log_2.take(1)