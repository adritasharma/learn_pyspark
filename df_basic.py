import pyspark
import pandas as pd

from pyspark.sql import SparkSession

# pd_df = pd.read_csv('test.csv')
# print(type(pd_df))

spark = SparkSession.builder.appName('LearnSpark').getOrCreate()

# # spark.sparkContext.setLogLevel('debug')



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

# Retrieving Column Names

print(df_spark.columns)

## Get Top 2 Records (in DataFrame format)

print(df_spark.head(2))

## Select one Column 

print(df_spark.select('Name'))

## Select Column with entire data

print(df_spark.select('Name').show())

### Select more than one Column 

print(df_spark.select(['Name','Age']).show())

### Column Data Types

print(df_spark.dtypes)

## Checking with Describe Options

print(df_spark.describe())

print(df_spark.describe().show())


### Adding Columns in Data Frame

df_spark.withColumn('Age after 5 Years', df_spark['Age'] + 5).show()

### Dropping Columns from Data Frame

df_spark.drop('Age').show()

### Rename Column in Data Frame

df_spark.withColumnRenamed('Name', 'Label').show()

