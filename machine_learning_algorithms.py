from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark ML example on titanic data").getOrCreate()
train_file_path = "/home/abin/my_works/github_works/spark-learning/dataset/train.csv"
# inferSchema = 'True', infers the input schema automatically from data.
titanic_df = spark.read.csv(train_file_path, header='True', inferSchema='True')
passengers_count = titanic_df.count()
print(passengers_count)
titanic_df.show(5)
titanic_df.describe().show()
