from pyspark.sql import SparkSession


def null_value_count(df):
    null_columns_count = []
    numRows = df.count()
    for k in df.columns:
        nullRows = df.where(titanic_df.colRegex(k).isNull()).count()
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

#Let's do some exploratory data analysis(EDA)
titanic_df.groupBy("Survived").count().show()
titanic_df.groupBy("Sex").count().show()
titanic_df.groupBy("Sex", "Survived").count().show()
titanic_df.groupBy("Pclass").count().show()
titanic_df.groupBy("Pclass", "Survived").count().show()

null_value_count_list = null_value_count(titanic_df)
spark.createDataFrame(null_value_count_list, ['Column_With_Null_Value', 'Null_Values_Count']).show()




