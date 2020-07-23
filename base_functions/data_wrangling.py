import datetime

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("Wrangling data") \
    .getOrCreate()

path = "/home/abin/my_works/github_works/spark-learning/dataset/sparkify_log_small.json"
user_log = spark.read.json(path)

# Data Exploration
user_log.take(5)
user_log.printSchema()
user_log.describe().show()
user_log.describe("artist").show()
user_log.describe("sessionId").show()
user_log.count()
user_log.select("page").dropDuplicates().sort("page").show()
user_log.select(["userId", "firstName", "page", "song"]).where(user_log.userId == "1046").collect()

# Calculating statistics by hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x /1000.0).hour)
user_log= user_log.withColumn("hour", get_hour(user_log.ts))

user_log.head()

songs_in_hour = user_log.filter(user_log.page == "NextSong").groupBy(user_log.hour).count().orderBy(user_log.hour.cast("float"))
songs_in_hour.show()

songs_in_hour_pd = songs_in_hour.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)
plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs Played")
plt.show()


# Drop rows with missing values
user_log_valid = user_log.dropna(how="any", subset=["userId", "sessionId"])
user_log_valid.count()
user_log.select("userId").dropDuplicates().sort("userId").show()
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
user_log_valid.count()

# Users downgrade their accounts
# Find when  users downgrade their accounts and then flag those log entries.
user_log_valid.filter("page= 'Submit Downgrade'").show()
user_log.select(["userId", "firstname", "page", "level", "song"]).where(user_log.userId == "1138").collect()
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())
user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))
user_log_valid.head()

# Use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
windowval = Window.partitionBy("userId").orderBy(F.desc("ts")).rangeBetween(Window.unboundedPreceding, 0)
user_log_valid = user_log_valid.withColumn("phase", F.sum("downgraded").over(windowval))
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log.userId == "1138")\
    .sort("ts").collect()
user_log_valid.show()



