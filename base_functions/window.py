import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as F


spark = SparkSession \
    .builder\
    .appName("window_fn")\
    .master("local[*]")\
    .getOrCreate()

# Window
d = {'A':['a','b','c','d', 'e', 'f'],'B':['m','m','n','n', 'm', 'n'],'C':[1,2,3,6,5,7]}
dp = pd.DataFrame(d)
df = spark.createDataFrame(dp)

w = Window.partitionBy('B').orderBy(df.C.desc())
df = df.withColumn('rank', F.rank().over(w))
df.show()

product_revenue = {"product": ["Thin", "Normal", "Mini", "Ultra thin", "Very thin", "Big"
    , "Bendable", "Foldable", "Pro", "Pro2"], "category": ["Cell phone", "Tablet", "Tablet"
    , "Cell phone", "Cell phone", "Tablet", "Cell phone", "Cell phone", "Tablet", "Tablet"]
    , "revenue": [6000, 1500, 5500, 5000, 6000, 2500, 3000, 3000, 4500, 6500]}

dp = pd.DataFrame(product_revenue)
df = spark.createDataFrame(dp)
#
# # Find top 2 products from each category
w = Window.partitionBy("category").orderBy(df.revenue.desc())
df = df.withColumn('rank', F.rank().over(w))
df.show()
df.filter(df.rank <= 2).show()


shopping_data = \
[('Alex','2018-10-10','Paint',80),('Alex','2018-04-02','Ladder',20),('Alex','2018-06-22','Stool',20),\
('Alex','2018-12-09','Vacuum',40),('Alex','2018-07-12','Bucket',5),('Alex','2018-02-18','Gloves',5),\
('Alex','2018-03-03','Brushes',30),('Alex','2018-09-26','Sandpaper',10)]

df = spark.createDataFrame(shopping_data, ["name", "date", "product", "price"])\
    .withColumn("date", F.col('date').cast(DateType()))

df.show()
df.printSchema()

W0 = Window.partitionBy("name")

# Continuous ranking, skip rankings, tiles, relative/percent rank
df.withColumn("price_rank", F.dense_rank().over(W0.orderBy(F.col("price").desc()))).show()


df.withColumn("price_rank", F.rank().over(W0.orderBy(F.col("price").asc()))).show()

df.withColumn("price_rank", F.ntile(4).over(W0.orderBy(F.col("price").desc()))).show()

df.withColumn("price_rank", F.percent_rank().over(W0.orderBy(F.col("price").desc()))).show()


# Average, Sum, Max, Max within Rows, Counts
df.withColumn("avg_to_date", F.round(F.avg('price').over(W0.orderBy(F.col("date"))), 2))\
    .withColumn("accumulating_sum", F.sum("price").over(W0.orderBy(F.col("date"))))\
    .withColumn("max_to_date", F.max("price").over(W0.orderBy(F.col("date"))))\
    .withColumn("max_of_last2", F.max("price").over(W0.orderBy(F.col("date")).rowsBetween(-1, Window.currentRow)))\
    .withColumn("items_to_date", F.count("price").over(W0.orderBy(F.col("date"))))\
    .show()


# Row item difference
df.withColumn("days_from_last_purchase", F.datediff("date", F.lag("date", 1).over(W0.orderBy(F.col("date")))))\
    .withColumn("days_before_next_purchase", F.datediff(F.lead("date", 1).over(W0.orderBy(F.col("date"))), "date"))\
    .show()


# Aggregations Lists and Sets
new_row = spark.createDataFrame([("Alex", "2018-10-11", "Paint", 80)])
df2 = df.union(new_row)

df2.withColumn("items_by_price", F.collect_list("product").over(W0.partitionBy("price")))\
    .withColumn("all_prices", F.collect_set("price").over(W0)).show()

df2.withColumn("items", F.collect_set("product").over(W0.partitionBy("price")))\
    .select("name", "Price", "items")\
    .distinct()\
    .show()


# Time series - Moving Average
# This is another slightly abstract idea along the lines of lag and leap. In this case we use the current
#  row against a user defined range (e.g. 30 day buffer) in order to find a numerical value (e.g. mean)
#  with the specified range.

days = lambda i: i * 86400 # 86400 seconds in a day

df.withColumn("unix_time", F.col("date").cast("timestamp").cast("long"))\
    .withColumn("30day_moving_avg", F.avg("price").over(W0.orderBy(F.col("unix_time")).rangeBetween(-days(30), 0)))\
    .withColumn("45day_moving stdv", F.stddev("price").over(W0.orderBy(F.col("unix_time")).rangeBetween(-days(30), days(15))))\
    .show()



