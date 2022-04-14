from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    NullType,
    ArrayType,
)
from pytest import raises
from pysparkschema.types import merge_schemas, TypeMergeError


def test_merge_schemas():
    schema = StructType(
        [
            StructField("test1", StringType(), True),
            StructField("test2", NullType(), True),
            StructField("test3", StringType(), True),
            StructField("test4", NullType(), True),
            StructField("s1", NullType(), True),
            StructField(
                "nest1",
                StructType(
                    [
                        StructField("test1", StringType(), True),
                        StructField("test2", NullType(), True),
                        StructField("test3", StringType(), True),
                        StructField("test4", NullType(), True),
                        StructField("s1", NullType(), True),
                        StructField(
                            "nest2",
                            StructType(
                                [
                                    StructField("test1", StringType(), True),
                                    StructField("test2", NullType(), True),
                                    StructField("test3", StringType(), True),
                                    StructField("test4", NullType(), True),
                                    StructField("s1", NullType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                "array1",
                ArrayType(
                    StructType(
                        [
                            StructField("test1", StringType(), True),
                            StructField("test2", NullType(), True),
                            StructField("test3", StringType(), True),
                            StructField("test4", NullType(), True),
                            StructField("s1", NullType(), True),
                            StructField(
                                "nest2",
                                StructType(
                                    [
                                        StructField("test1", StringType(), True),
                                        StructField("test2", NullType(), True),
                                        StructField("test3", StringType(), True),
                                        StructField("test4", NullType(), True),
                                        StructField("s1", NullType(), True),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
            ),
            StructField("array2", ArrayType(NullType())),
            StructField("array3", ArrayType(StringType())),
            StructField("array4", ArrayType(NullType())),
            StructField("array5", ArrayType(ArrayType(NullType()))),
            StructField("array6", ArrayType(ArrayType(StringType()))),
            StructField("array7", ArrayType(ArrayType(NullType()))),
        ]
    )
    schema2 = StructType(
        [
            StructField("test1", StringType(), True),
            StructField("test2", StringType(), True),
            StructField("test3", NullType(), True),
            StructField("test4", NullType(), True),
            StructField("s2", NullType(), True),
            StructField(
                "nest1",
                StructType(
                    [
                        StructField("test1", StringType(), True),
                        StructField("test2", StringType(), True),
                        StructField("test3", NullType(), True),
                        StructField("test4", NullType(), True),
                        StructField("s2", NullType(), True),
                        StructField(
                            "nest2",
                            StructType(
                                [
                                    StructField("test1", StringType(), True),
                                    StructField("test2", StringType(), True),
                                    StructField("test3", NullType(), True),
                                    StructField("test4", NullType(), True),
                                    StructField("s2", NullType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                "array1",
                ArrayType(
                    StructType(
                        [
                            StructField("test1", StringType(), True),
                            StructField("test2", StringType(), True),
                            StructField("test3", NullType(), True),
                            StructField("test4", NullType(), True),
                            StructField("s2", NullType(), True),
                            StructField(
                                "nest2",
                                StructType(
                                    [
                                        StructField("test1", StringType(), True),
                                        StructField("test2", StringType(), True),
                                        StructField("test3", NullType(), True),
                                        StructField("test4", NullType(), True),
                                        StructField("s2", NullType(), True),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
            ),
            StructField("array2", ArrayType(NullType())),
            StructField("array3", ArrayType(NullType())),
            StructField("array4", ArrayType(StringType())),
            StructField("array5", ArrayType(ArrayType(NullType()))),
            StructField("array6", ArrayType(ArrayType(NullType()))),
            StructField("array7", ArrayType(ArrayType(StringType()))),
        ]
    )

    merge_result = merge_schemas(schema, schema2)
    assert merge_result


def test_error_merge_schemas():
    schema = StructType(
        [
            StructField("test1", StringType(), True),
        ]
    )
    schema2 = StructType(
        [
            StructField("test1", ArrayType(StringType()), True),
        ]
    )
    with raises(TypeMergeError):
        merge_schemas(schema, schema2)
