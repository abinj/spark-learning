from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.ml.feature import Tokenizer

spark = SparkSession.builder.appName("spark-text-process").getOrCreate()

sms = spark.read.csv('../dataset/sms.csv', header=True, sep=';')
sms.printSchema()

# Remove punctuation (REGEX provided) and numbers
wrangled = sms.withColumn('text', regexp_replace(sms.text, '[_():;,.!?\\-]', " "))
wrangled = wrangled.withColumn('text', regexp_replace(wrangled.text, '[0-9]', " "))

# Merge multiple spaces
wrangled = wrangled.withColumn('text', regexp_replace(wrangled.text, ' +', " "))

#Split the text into words
wrangled = Tokenizer(inputCol='text', outputCol='words').transform(wrangled)

wrangled.show(4, truncate=False)