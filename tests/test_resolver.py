from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    NullType,
)
from pysparkschema.resolver import (
    NullTypeResolveStrategy,
    NumberTypeResolveStrategy,
    is_string,
    is_number,
)


def test_null_type_resolve_strategy():
    assert NullTypeResolveStrategy.resolve(NullType(), NullType()) is None
    assert is_string(NullTypeResolveStrategy.resolve(StringType(), NullType()))
    assert is_string(NullTypeResolveStrategy.resolve(NullType(), StringType()))
    assert is_number(NullTypeResolveStrategy.resolve(IntegerType(), NullType()))
    assert is_number(NullTypeResolveStrategy.resolve(NullType(), IntegerType()))
    assert is_number(NullTypeResolveStrategy.resolve(DoubleType(), NullType()))
    assert is_number(NullTypeResolveStrategy.resolve(NullType(), DoubleType()))


def test_number_type_resolve_strategy():
    assert is_string(NumberTypeResolveStrategy.resolve(StringType(), IntegerType()))
    assert is_string(NumberTypeResolveStrategy.resolve(IntegerType(), StringType()))
    assert is_string(NumberTypeResolveStrategy.resolve(StringType(), DoubleType()))
    assert is_string(NumberTypeResolveStrategy.resolve(DoubleType(), StringType()))
    assert NumberTypeResolveStrategy.resolve(StringType(), StringType()) is None
    assert NumberTypeResolveStrategy.resolve(NullType(), NullType()) is None
    assert NumberTypeResolveStrategy.resolve(StringType(), NullType()) is None
    assert NumberTypeResolveStrategy.resolve(NullType(), StringType()) is None
