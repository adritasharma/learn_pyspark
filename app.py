import pyspark
import pandas as pd

from pyspark.sql import SparkSession

pd_df = pd.read_csv('test.csv')
print(type(pd_df))

spark = SparkSession.builder.appName('LearnSpark').getOrCreate()

# spark.sparkContext.setLogLevel('debug')



## Reading Data

df_spark = spark.read.csv('test.csv')
print(df_spark.show())

df_spark = spark.read.option('header', 'true').csv('test.csv')
print(df_spark.show())

## Reading Column Details

print(df_spark)


## Reading DataFrame schema

df_spark.printSchema()

df_spark = spark.read.option('header', 'true').csv('test.csv', inferSchema=True)

df_spark.printSchema()

## Shorthand

df_spark = spark.read.csv('test.csv', header=True, inferSchema=True)

df_spark.printSchema()

print(type(df_spark))


