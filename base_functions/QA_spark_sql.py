from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("qa_spark_sql") \
    .getOrCreate()

user_log = spark.read.json("/home/abin/my_works/github_works/spark-learning/dataset/sparkify_log_small.json")
user_log.createOrReplaceTempView("log_table")

# Q1: Which page did user id "" (empty string) NOT visit?

user_log.printSchema()

spark.sql('''
        SELECT *
        FROM(SELECT DISTINCT page
            FROM log_table
            WHERE userID='') AS user_pages
        RIGHT JOIN(
            SELECT DISTINCT page
            FROM log_table) AS all_pages
        ON user_pages.page = all_pages.page
        WHERE user_pages.page IS NULL
        ''').show()

# Q2: How many female users do we have in the data set?

spark.sql('''
        SELECT COUNT(DISTINCT userID)
        FROM log_table
        WHERE gender = 'F'
        ''').show()


# Q3: How many songs were played from the most played artist?

spark.sql('''
        SELECT Artist, COUNT(Artist) AS plays
        FROM log_table
        GROUP BY Artist
        ORDER BY plays DESC
        LIMIT 1
        ''').show()

# Q4: How many songs do users listen to on average between visiting our home page?

is_home = spark.sql('''
    SELECT userID, page, ts, CASE
    WHEN page = 'Home' THEN 1 ELSE 0
    END AS is_home 
    FROM log_table
    WHERE (page = 'NextSong') or (page = 'Home')
    ''')

is_home.createOrReplaceTempView("is_home_table")
cumulative_sum = spark.sql('''
    SELECT *, SUM(is_home) 
    OVER(PARTITION BY userID 
    ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS period
    FROM is_home_table
    ''')

cumulative_sum.createOrReplaceTempView("period_table")
spark.sql('''
    SELECT AVG(count_results) FROM 
    (SELECT COUNT(*) AS count_results FROM period_table
    GROUP BY userID, period, page HAVING page = 'NextSong') AS counts
    ''').show()


