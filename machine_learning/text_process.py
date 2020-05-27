from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

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

# Remove stop words.
wrangled = StopWordsRemover(inputCol='words', outputCol='terms') \
    .transform(wrangled)

# Apply the hashing trick
wrangled = HashingTF(inputCol='terms', outputCol='hash', numFeatures=1024) \
    .transform(wrangled)

# Convert hashed symbols to TF-IDF
tf_idf = IDF(inputCol='hash', outputCol='features') \
    .fit(wrangled).transform(wrangled)

tf_idf.select('terms', 'features').show(4, truncate=False)


# # Split the data into training and testing sets
# sms_train, sms_test = tf_idf.randomSplit([0.8, 0.2], seed=13)
#
# # Fit a Logistic Regression model to the training data
# logistic = LogisticRegression(regParam=0.2).fit(sms_train)
#
# # Make predictions on the testing data
# prediction = logistic.transform(sms_test)
#
# # Create a confusion matrix, comparing predictions to known labels
# prediction.groupBy('label', 'prediction').count().show()