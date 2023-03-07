from pyspark.sql import SparkSession

from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('LearnSpark').getOrCreate()

### Reading Data

df_spark = spark.read.csv('test_data.csv', header=True, inferSchema=True)
df_spark.show()

### Drop Specific Rows with empty or null values

df_spark.na.drop().show()

## Filling missing value  
 
df_spark.na.fill('NA').show() 

### Filling missing value in specific columns  

df_spark.na.fill('Missing Value', ['Age','Salary']).show() 


### Filling missing columns value with mean/median 

imputer = Imputer(
    inputCols=['Age', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['Age', 'Salary']]
    ).setStrategy("mean")

# Add imputation cols to df

imputer.fit(df_spark).transform(df_spark).show()