from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName("Data wrangling practice") \
    .getOrCreate()

df = spark.read.json("/home/abin/my_works/github_works/spark-learning/dataset/sparkify_log_small.json")

# Q1: Which page did user id "" (empty string) NOT visit?

df.printSchema
blank_pages = df.filter(df.userId == '') \
    .select(F.col('page')) \
    .alias('black_pages') \
    .dropDuplicates()

# get a list of possible pages that could be visited
all_pages = df.select("page").dropDuplicates()

# Find values in all_pages that are not in black_pages
# these are the pages that the blank user did not go to
for row in set(all_pages.collect()) - set(blank_pages.collect()):
    print(row.page)


# Q2: How many female users do we have in the data set?

print(df.filter(df.gender == 'F') \
    .select('userId', 'gender') \
    .dropDuplicates() \
    .count())

# Q3: How many songs were played from the most played artist?

df.filter(df.page == "NextSong") \
    .select("Artist") \
    .groupBy("Artist") \
    .agg({"Artist": "count"}) \
    .withColumnRenamed("count(Artist)", "Artistcount") \
    .sort(F.desc("Artistcount")) \
    .show(1)


# Q4: How many songs do users listen to on average between visiting our home page?

function = F.udf(lambda ishome : int(ishome == 'Home'), IntegerType())

user_window = Window \
    .partitionBy('userID') \
    .orderBy(F.desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', function(F.col('page'))) \
    .withColumn('period', F.sum('homevisit').over(user_window))

cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}).show()




