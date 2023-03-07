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

# DataFrame Basics

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
    df_spark.show()

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

### Retrieving Column Names

    print(df_spark.columns)

**Output**: 

    ['Name', 'City', 'Age']

### Get Top 2 Records (in DataFrame format)

    print(df_spark.head(2))

**Output**: 

    [Row(Name='Adrita', City='Bangalore', Age=30), Row(Name='Deepika', City='Kolkata', Age=29)]

### Select one Column 

    df_spark.select('Name')

**Output**: 

    DataFrame[Name: string]

### Select Column with entire data

    df_spark.select('Name').show()

**Output**: 

    +-------+
    |   Name|
    +-------+
    | Adrita|
    |Deepika|
    | Ankita|
    +-------+

### Select more than one Column 

    df_spark.select(['Name','Age']).show()

**Output**: 

    +-------+---+
    |   Name|Age|
    +-------+---+
    | Adrita| 30|
    |Deepika| 29|
    | Ankita| 28|
    +-------+---+

### Column Data Types

    print(df_spark.dtypes)

**Output**: 

    [('Name', 'string'), ('City', 'string'), ('Age', 'int')]

### Checking with Describe Options

    df_spark.describe()

**Output**:  

    DataFrame[summary: string, Name: string, City: string, Age: string]

### Checking with Describe Options Show

Here we get summary column.

    df_spark.describe().show()

**Output**:  

    +-------+-------+---------+----+
    |summary|   Name|     City| Age|
    +-------+-------+---------+----+
    |  count|      3|        3|   3|
    |   mean|   null|     null|29.0|
    | stddev|   null|     null| 1.0|
    |    min| Adrita|Bangalore|  28|
    |    max|Deepika|  Kolkata|  30|
    +-------+-------+---------+----+



### Adding Columns in Data Frame

    df_spark.withColumn('Age after 5 Years', df_spark['Age'] + 5).show()

**Output**: 

    +-------+---------+---+-----------------+
    |   Name|     City|Age|Age after 5 Years|
    +-------+---------+---+-----------------+
    | Adrita|Bangalore| 30|               35|
    |Deepika|  Kolkata| 29|               34|
    | Ankita| Guwahati| 28|               33|
    +-------+---------+---+-----------------+


### Dropping Columns from Data Frame

df_spark.drop('Age').show() 

**Output**: 

    +-------+---------+
    |   Name|     City|
    +-------+---------+
    | Adrita|Bangalore|
    |Deepika|  Kolkata|
    | Ankita| Guwahati|
    +-------+---------+

### Rename Column in Data Frame

    df_spark.withColumnRenamed('Name', 'Label').show() 

**Output**: 

    +-------+---------+---+
    |  Label|     City|Age|
    +-------+---------+---+
    | Adrita|Bangalore| 30|
    |Deepika|  Kolkata| 29|
    | Ankita| Guwahati| 28|
    +-------+---------+---+

# DataFrame Missing Value Operation   

### Reading Data with empty values


df_spark = spark.read.csv('test_data.csv', header=True, inferSchema=True)
df_spark.show()

**Output**:

    +-----+---------+----+------+
    | Name|     City| Age|Salary|
    +-----+---------+----+------+
    | Riya|Bangalore|  30| 20000|
    | Rupa|  Kolkata|  29| 30000|
    |Sonai| Guwahati|  28| 25000|
    | null|     null|null|  null|
    | null|  Kolkata|  33| 55000|
    |Guddu|     null|  30| 60000|
    +-----+---------+----+------+


### Drop Specific Rows with null value

It drops all the rows that have any null value

    df_spark.na.drop().show() 

**Output**:

    +-----+---------+---+------+
    | Name|     City|Age|Salary|
    +-----+---------+---+------+
    | Riya|Bangalore| 30| 20000|
    | Rupa|  Kolkata| 29| 30000|
    |Sonai| Guwahati| 28| 25000|
    +-----+---------+---+------+    

Signature

    df_spark.na.drop(how='any', thres=None, subset=None)

_ **how** - any/all    
    any - Drop a row if it contains any nulls
    all - Drop a row if all its values are  null

_ **thres** - None/number  
    
    thres=2 - atleast 2 non-null values should be presesnt in a row

_ **subset** - None/[columnName]  
    
    subset=['Age'] - if Age has null value, the whole row will get deleted

### Filling missing value

    df_spark.na.fill('NA').show() 

    Note: Adding `inferSchema=True` doesn't consider intergers while filling

**Output**:

    +-----+---------+----+------+
    | Name|     City| Age|Salary|
    +-----+---------+----+------+
    | Riya|Bangalore|  30| 20000|
    | Rupa|  Kolkata|  29| 30000|
    |Sonai| Guwahati|  28| 25000|
    |   NA|       NA|null|  null|
    |   NA|  Kolkata|  33| 55000|
    |Guddu|       NA|  30| 60000|
    +-----+---------+----+------+

### Filling missing value in specific columns  

    df_spark = spark.read.csv('test_data.csv', header=True)

    df_spark.na.fill('Missing Value', ['Age','Salary']).show() 


**Output**:

    +-----+---------+-------------+-------------+
    | Name|     City|          Age|       Salary|
    +-----+---------+-------------+-------------+
    | Riya|Bangalore|           30|        20000|
    | Rupa|  Kolkata|           29|        30000|
    |Sonai| Guwahati|           28|        25000|
    | null|     null|Missing Value|Missing Value|
    | null|  Kolkata|           33|        55000|
    |Guddu|     null|           30|        60000|
    +-----+---------+-------------+-------------+


### Filling missing columns value with mean/median 


**Import Imputer**:

    from pyspark.ml.feature import Imputer

-
    df_spark = spark.read.csv('test_data.csv', header=True, inferSchema=True)

    imputer = Imputer(
        inputCols=['Age', 'Salary'], 
        outputCols=["{}_imputed".format(c) for c in ['Age', 'Salary']]
        ).setStrategy("median")

Add imputation cols to df. Here null will be replaced bu the mean value of the respective column

    imputer.fit(df_spark).transform(df_spark).show()  

**Output**:

    +-----+---------+----+------+-----------+--------------+
    | Name|     City| Age|Salary|Age_imputed|Salary_imputed|
    +-----+---------+----+------+-----------+--------------+
    | Riya|Bangalore|  30| 20000|         30|         20000|
    | Rupa|  Kolkata|  29| 30000|         29|         30000|
    |Sonai| Guwahati|  28| 25000|         28|         25000|
    | null|     null|null|  null|         30|         38000|
    | null|  Kolkata|  33| 55000|         33|         55000|
    |Guddu|     null|  30| 60000|         30|         60000|
    +-----+---------+----+------+-----------+--------------+


# DataFrame Filter Operation   

### 
