# Pyspark

Getting started with PySpark 

Ref: https://www.youtube.com/watch?v=_C8kWso4ne4

## Spark  – Overview

Apache Spark is a lightning fast real-time processing framework. 

Apache Spark has its own cluster manager, where it can host its application. It leverages Apache Hadoop for both storage and processing.

- It does in-memory computations to analyze data in real-time
- It does batch processing
- It supports interactive queries and iterative algorithms

## PySpark

Apache Spark is written in Scala programming language. To support Python with Spark, Apache Spark Community released a tool, PySpark. It uses a library called Py4j.

PySpark offers PySpark Shell which links the Python API to the spark core and initializes the Spark context.

## Installation

Prerequisite: Install Java and Hadoop

https://towardsdatascience.com/installing-hadoop-3-2-1-single-node-cluster-on-windows-10-ac258dd48aef

    pip install pyspark

## Setting up session

- To work with PiSpark, we need to start a Spark Session

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName('LearnSpark').getOrCreate() 

### DataFrame

DataFrame is a Data Structure in which we can perform various kinds of operations.

- Pandas Dataframe

    pd_df = pd.read_csv('test.csv')
    print(type(pd_df))

**Output**: 

    <class 'pandas.core.frame.DataFrame'>

- PiSpark Dataframe

        df_spark = spark.read.csv('test.csv')
        print(type(df_spark))

**Output**: 

    <class 'pyspark.sql.dataframe.DataFrame'>        


## Reading a CSV

    df_spark = spark.read.csv('test.csv')
    print(df_spark.show())

**Output**:  

    +-------+---------+---+
    |    _c0|      _c1|_c2|
    +-------+---------+---+
    |   Name|     City|Age|
    | Adrita|Bangalore| 30|
    |Deepika|  Kolkata| 29|
    | Ankita| Guwahati| 28|
    +-------+---------+---+

**Reading Header**:

    df_spark = spark.read.option('header', 'true').csv('test.csv')

**Display Column Details**:  

    print(df_spark)

**Output**:  

    DataFrame[Name: string, City: string]

**Display entire Data Set**:  

    print(df_spark.show())

**Output**:  

    +-------+---------+---+
    |   Name|     City|Age|
    +-------+---------+---+
    | Adrita|Bangalore| 30|
    |Deepika|  Kolkata| 29|
    | Ankita| Guwahati| 28|
    +-------+---------+---+

### Reading DataFrame schema

    root
     |-- Name: string (nullable = true)
     |-- City: string (nullable = true)
     |-- Age: string (nullable = true)

Here, even though Age is an integer, it  is taking Age as a string field. By default all the columns in the schema are taken as string. To get the actual type, we have to add `inferSchema=True`  

    df_spark = spark.read.option('header', 'true').csv('test.csv', inferSchema=True)

    df_spark.printSchema()

**Output**: 

    root
     |-- Name: string (nullable = true)
     |-- City: string (nullable = true)
     |-- Age: integer (nullable = true)

**Shorthand to include header and inferSchema**: 

    df_spark = spark.read.csv('test.csv', header=True, inferSchema=True)






