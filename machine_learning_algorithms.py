from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession, column
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit


def null_value_count(df):
    null_columns_count = []
    numRows = df.count()
    for k in df.columns:
        nullRows = df.where(col(k).isNull()).count()
        if nullRows > 0:
            temp = k, nullRows
            null_columns_count.append(temp)
    return null_columns_count

spark = SparkSession.builder.appName("Spark ML example on titanic data").getOrCreate()
train_file_path = "/home/abin/my_works/github_works/spark-learning/dataset/train.csv"
# inferSchema = 'True', infers the input schema automatically from data.
titanic_df = spark.read.csv(train_file_path, header='True', inferSchema='True')
passengers_count = titanic_df.count()
print(passengers_count)
titanic_df.show(5)
titanic_df.describe().show()
titanic_df.printSchema()
titanic_df.select("Survived", "Pclass", "Embarked").show()

# Let's do some exploratory data analysis(EDA)
titanic_df.groupBy("Survived").count().show()
titanic_df.groupBy("Sex").count().show()
titanic_df.groupBy("Sex", "Survived").count().show()
titanic_df.groupBy("Pclass").count().show()
titanic_df.groupBy("Pclass", "Survived").count().show()

null_value_count_list = null_value_count(titanic_df)
spark.createDataFrame(null_value_count_list, ['Column_With_Null_Value', 'Null_Values_Count']).show()

mean_age = titanic_df.select(mean('Age')).collect()[0][0]
print(mean_age)

# We extract initials from the name, string which lie between A-Z or a-z and followed by a dot(.)
titanic_df = titanic_df.withColumn("Initial", regexp_extract(col("Name"), "([A-Za-z]+)\.", 1))
titanic_df.select("Initial").distinct().show()
titanic_df = titanic_df.replace(['Mlle', 'Mme', 'Ms', 'Dr', 'Major', 'Lady', 'Countess', 'Jonkheer', 'Col', 'Rev'
                                    , 'Capt', 'Sir', 'Don'], ['Miss', 'Miss', 'Miss', 'Mr', 'Mr', 'Mrs', 'Mrs'
    , 'Other', 'Other', 'Other', 'Mr', 'Mr', 'Mr'])
titanic_df.select("Initial").distinct().show()
titanic_df.groupBy("Initial").avg("Age").collect()

# 22 is the average of people with Initial as Miss from the dataset
titanic_df = titanic_df.withColumn("Age", when((titanic_df["Initial"] == "Miss") & (titanic_df["Age"].isNull())
                                               , 22).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age", when((titanic_df["Initial"] == "Other") & (titanic_df["Age"].isNull())
                                               , 46).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age", when((titanic_df["Initial"] == "Master") & (titanic_df["Age"].isNull())
                                               , 5).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age", when((titanic_df["Initial"] == "Mr") & (titanic_df["Age"].isNull())
                                               , 33).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age", when((titanic_df["Initial"] == "Mrs") & (titanic_df["Age"].isNull())
                                               , 36).otherwise(titanic_df["Age"]))

titanic_df.filter(titanic_df.Age == 46).select("Initial").show()
titanic_df.select("Age").show()

titanic_df.groupBy("Embarked").count().show()

# Majority of passengers boarded from "S", we can impute with "S"
titanic_df = titanic_df.na.fill({"Embarked": "S"})

# we can drop Cabin feature as it has lots of null values
titanic_df = titanic_df.drop("Cabin")
titanic_df.printSchema()

titanic_df = titanic_df.withColumn("Family-Size", col('SibSp') + col('Parch'))
titanic_df.groupBy("Family-Size").count().show()
titanic_df = titanic_df.withColumn('Alone', lit(0))
titanic_df = titanic_df.withColumn("Alone", when(titanic_df["Family-Size"] == 0, 1).otherwise(titanic_df["Alone"]))
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(titanic_df) for column in ["Sex", "Embarked", "Initial"]]
pipeline = Pipeline(stages=indexers)
titanic_df = pipeline.fit(titanic_df).transform(titanic_df)
titanic_df.show()
titanic_df.printSchema()

# Drop columns which are not required
titanic_df = titanic_df.drop("PassengerId", "Name", "Ticket", "Cabin", "Embarked", "Sex", "Initial")
titanic_df.show()

feature = VectorAssembler(inputCols=titanic_df.columns[1:], outputCol="features")
feature_vector = feature.transform(titanic_df)

feature_vector.show()


# Now the data is all set, let's split it into training  and test, I'll using 80%,20% approach
(trainingData, testData) = feature_vector.randomSplit([0.8, 0.2], seed=11)

# Logistic Regression
lr = LogisticRegression(labelCol="Survived", featuresCol="features")

#Training algorithm
lrModel = lr.fit(trainingData)
lr_prediction = lrModel.transform(testData)
lr_prediction.select("prediction", "Survived", "features").show()

#Evaluation
evaluator = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="accuracy")
lr_accuracy = evaluator.evaluate(lr_prediction)
print("Accuracy of LogisticRegression is = %g"% (lr_accuracy))
print("Test Error of LogisticRegression = %g " % (1.0 - lr_accuracy))



#Decision Tree Classifier
dt = DecisionTreeClassifier(labelCol="Survived", featuresCol="features")
dt_model = dt.fit(trainingData)
dt_prediction = dt_model.transform(testData)
dt_prediction.select("prediction", "Survived", "features").show()

#Evaluation
dt_accuracy = evaluator.evaluate(dt_prediction)
print("Accuracy of decision tree classifier is = %g"% (dt_accuracy))
print("Test error of decision tree classifier = %g" % (1.0 - dt_accuracy))

#Randon Forest Classifier
rf = RandomForestClassifier(labelCol="Survived", featuresCol="features")
rf_model = rf.fit(trainingData)
rf_prediction = rf_model.transform(testData)
rf_prediction.select("prediction", "Survived", "features").show()

