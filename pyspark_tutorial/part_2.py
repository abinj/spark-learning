from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("part-2").getOrCreate()
df_pyspark = spark.read.csv("../dataset/flights.csv", header=True, inferSchema=True)

# Remove null values
df_pyspark.na.drop().show()

# Remove rows where all columns are null
df_pyspark.na.drop(how="all").show()


# Remove rows where less than 2 non null values in a row
df_pyspark.na.drop(how="all", thresh=2).show()

# subset, Remove rows where specific column values are none
df_pyspark.na.drop(how="all", subset=['mon']).show()


# Filling the missing value
df_pyspark.na.fill('Missing Values').show()


# Filling missing values with subset
df_pyspark.na.fill("Missing Values", ["mon", "dom"]).show()

# Filling missing values with Mean/Mode
from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['depart'],
    outputCols=["{}_imputed".format(c) for c in ['depart']]
).setStrategy("mean")

imputer.fit(df_pyspark).transform(df_pyspark).show()

# NOTE: Imputer can impute custom values other than ‘NaN’ by .setMissingValue(custom_value)

