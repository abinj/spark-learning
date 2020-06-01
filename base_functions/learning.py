import os

import numpy as np
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


SUBMIT_ARGS = "--packages mysql:mysql-connector-java:5.1.48 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS



spark = SparkSession \
    .builder\
    .appName("learning")\
    .master("local[*]")\
    .getOrCreate()

sc = spark.sparkContext

df = sc.parallelize([(1,2,3,'a b c'), (4,5,6,'d e f'), (7,8,9,'g h i')]).toDF(['col1', 'col2', 'col3', 'col4'])

df.show()

sc.parallelize([(1,2,3,'a b c'), (4,5,6,'d e f'), (7,8,9,'g h i')]).collect()

Employee = spark.createDataFrame([('1', 'Joe', '70000', '1'),
                               ('2', 'Henry', '80000', '2'),
                               ('3', 'Sam', '60000', '2'),
                               ('4', 'Max', '90000', '1')],
                              ['Id', 'Name', 'Salary', 'DepartmentId'])

Employee.show()
Employee.na.replace(['Joe', 'Henry', 'Sam', 'Max'], ['1', '2', '3', '4']).show()


df_csv = spark.read.csv("/home/abin/my_works/github_works/spark-learning/dataset/flights.csv", header=True, inferSchema=True)
df_csv.show()

user = "abin"
pw = "Learn4Joke@"

tableName = "flights"
url = "jdbc:mysql://localhost:3306/flights"
properties = {"driver": "com.mysql.jdbc.Driver", "dbtable": tableName, "user": user, "password": pw}
df_db = spark.read.jdbc(url=url, table = tableName,properties=properties)
df_db.show(5)
df_db.printSchema()
print(df_db.columns)
print(df_db.dtypes)
df_db[(df_db.dow < 4) & (df_db.mile > 1000)].show()
df_db.withColumn('Delay_norm', df_db.DELAY/df_db.groupBy().agg(F.sum("DELAY")).collect()[0][0]).show()
df_db.withColumn("cond", F.when((df_db.DELAY >= 0) & (df_db.DELAY < 10), 1)\
                 .when((df_db.DELAY >= 10) & (df_db.DELAY < 20), 2)\
                 .otherwise(3)).show()
df_db.withColumn("log_DELAY", F.when(df_db.DELAY > 0, F.log(df_db.DELAY)).otherwise(df_db.DELAY)).show()

d = {'A': [0, 1, 0],
'B': [1, 0, 1],
'C': [1, 0, 0]}

spark.createDataFrame(np.array(list(d.values())).T.tolist(), list(d.keys())).show()

