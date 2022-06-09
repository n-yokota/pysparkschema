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
from pysparkschema.types import merge_schemas

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

## Custom Resolver

You can use custom Resolver if you want to resolve some own pyspark type conflict cases.

```python
from pysparkschema.resolver import TypeResolveStrategy, TypeResolver
from pysparkschema.types import merge_schemas

class ForceFirstResolveStrategy(TypeResolveStrategy):
    @staticmethod
    def resolve(type1, type2):
        return type1

resolver = TypeResolver([ForceFirstResolveStrategy])
new_schema = merge_schemas(schema1, schema2, resolver)
```

## Using Re-constructor

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, NullType, ArrayType
from pyspark.sql import functions as F
from pysparkschema.reconstructor import reconstruct_to_new_schema

spark = SparkSession.builder.getOrCreate()
data = [(None, None, None, None)]

schema1 = StructType([
    StructField("test1",StringType(),True),
    StructField("test2",NullType(),True),
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
    StructField("test3",StringType(),True),
    StructField("array1", ArrayType(
        StructType([
            StructField("test1",StringType(),True),
            StructField("test2",StringType(),True),
            StructField("test3",StringType(),True),
        ]), True)
    ),
])

df = spark.createDataFrame(data=data, schema=schema1)
new_df = reconstruct_to_new_schema(df, schema2)
```
