from pyspark.sql import SparkSession

from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('LearnSpark').getOrCreate()

### Reading Data

df_pyspark = spark.read.csv('agg_data.csv', header=True, inferSchema=True)
df_pyspark.show()

## Group by

### Employees under each department

df_pyspark.groupBy('Department').count().show()


### Grouped to find the total salary of each department

df_pyspark.groupBy('Department').sum().show()

### Only  Aggregate    

df_pyspark.agg({'Salary':'sum'}).show()



