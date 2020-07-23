import datetime

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Data wrangling with  Spark SQL") \
    .getOrCreate()

path = "/home/abin/my_works/github_works/spark-learning/dataset/sparkify_log_small.json"
user_log = spark.read.json(path)

user_log.take(1)

user_log.printSchema()

user_log.createOrReplaceTempView("user_log_table")
spark.sql("SELECT * FROM user_log_table LIMIT 2").show()

spark.sql('''
        SELECT *
        FROM user_log_table
        LIMIT 2
        ''').show()

spark.sql('''
        SELECT COUNT(*)
        FROM user_log_table
        ''').show()

spark.sql('''
        SELECT userID, firstname, page, song
        FROM user_log_table
        WHERE userID == '1046'
        ''').collect()

spark.sql('''
        SELECT DISTINCT page
        FROM user_log_table
        ORDER BY page ASC
        ''').show()


# UDFs

spark.udf.register("get_hour", lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0).hour))
spark.sql('''
        SELECT *, get_hour(ts) AS hour
        FROM user_log_table
        LIMIT 1
        ''').collect()

songs_in_hour = spark.sql('''
    SELECT get_hour(ts) AS hour, COUNT(*)  as plays_per_hour
    FROM user_log_table
    WHERE page="NextSong"
    GROUP BY hour
    ORDER BY cast(hour as int) ASC
    ''')

songs_in_hour.show()


# Converting results to pandas

songs_in_hour_pd = songs_in_hour.toPandas()
print(songs_in_hour_pd)