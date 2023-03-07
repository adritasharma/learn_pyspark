from pyspark.sql import SparkSession

from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('LearnSpark').getOrCreate()

### Reading Data

df_pyspark = spark.read.csv('test_data.csv', header=True, inferSchema=True)
df_pyspark.show()


### Salary < 30000

df_pyspark.filter("Salary<=30000").show()

### Salary  > 20000 & < 30000

df_pyspark.filter((df_pyspark['Salary']<=30000) &
                  (df_pyspark['Salary']>=20000)).select(['Name','Salary']).show()


### NOT (~) Operation !Salary > 50000 

df_pyspark.filter(~(df_pyspark['Salary']>50000)).show()