# PySpark Schemas

## Installation

```python
pip install git+https://github.com/n-yokota/pysparkschema.git
```

## Testing

```python
pip install -e .[test]
pytest
```

## Usage

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, NullType, ArrayType

spark = SparkSession.builder.getOrCreate()

data = [(None, None, None, None)]

schema1 = StructType([
    StructField("test1",StringType(),True),
    StructField("test2",NullType(),True),
    StructField("test3",NullType(),True),
    StructField("array1", ArrayType(
        StructType([
            StructField("test1",StringType(),True),
            StructField("test2",NullType(),True),
        ]), True)
    ),
])
schema2 = StructType([
    StructField("test1",StringType(),True),
    StructField("test2",StringType(),True),
    StructField("test4",NullType(),True),
    StructField("array1", ArrayType(
        StructType([
            StructField("test1",StringType(),True),
            StructField("test2",StringType(),True),
        ]), True)
    ),
])
 

df1 = spark.createDataFrame(data=data,schema=schema1)
print("schema1")
df1.printSchema()
df2 = spark.createDataFrame(data=data,schema=schema2)
print("schema2")
df2.printSchema()
df3 = spark.createDataFrame(data=[],schema=merge_schemas(df1.schema, df2.schema))
print("merged schema")
df3.printSchema()
```

The result is as follows

```
schema1
root
 |-- test1: string (nullable = true)
 |-- test2: null (nullable = true)
 |-- test3: null (nullable = true)
 |-- array1: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- test1: string (nullable = true)
 |    |    |-- test2: null (nullable = true)

schema2
root
 |-- test1: string (nullable = true)
 |-- test2: string (nullable = true)
 |-- test4: null (nullable = true)
 |-- array1: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- test1: string (nullable = true)
 |    |    |-- test2: string (nullable = true)

merged schema
root
 |-- test1: string (nullable = true)
 |-- test2: string (nullable = true)
 |-- test3: null (nullable = true)
 |-- array1: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- test1: string (nullable = true)
 |    |    |-- test2: string (nullable = true)
 |-- test4: null (nullable = true)
```
