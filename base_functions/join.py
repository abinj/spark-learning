import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder\
    .appName("learning")\
    .master("local[*]")\
    .getOrCreate()


leftp = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                      'B': ['B0', 'B1', 'B2', 'B3'],
                      'C': ['C0', 'C1', 'C2', 'C3'],
                      'D': ['D0', 'D1', 'D2', 'D3']},
                     index=[0, 1, 2, 3])

rightp = pd.DataFrame({'A': ['A0', 'A1', 'A6', 'A7'],
                       'F': ['B4', 'B5', 'B6', 'B7'],
                       'G': ['C4', 'C5', 'C6', 'C7'],
                       'H': ['D4', 'D5', 'D6', 'D7']},
                       index=[4, 5, 6, 7])

lefts = spark.createDataFrame(leftp)
rights = spark.createDataFrame(rightp)

#Left Join
lefts.join(rights, on='A', how='left')\
    .orderBy('A', ascending=True).show()

# Right Join
lefts.join(rights, on='A', how='right')\
    .orderBy('A', ascending=True).show()

