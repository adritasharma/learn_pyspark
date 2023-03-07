from pyspark.sql import SparkSession

from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('LearnSpark').getOrCreate()

### Reading Data

df_pyspark = spark.read.csv('test_data.csv', header=True, inferSchema=True)
df_pyspark.show()


### Salary < 30000