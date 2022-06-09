from pyspark.sql.types import StructType, StructField, ArrayType
from .resolver import (
    TypeResolver,
    NullTypeResolveStrategy,
    NumberTypeResolveStrategy,
    is_array,
    is_struct,
)
from .error import TypeMergeError


default_resolver = TypeResolver(
    [
        NullTypeResolveStrategy,
        NumberTypeResolveStrategy,
    ]
)


def merge_array_schemas(a1, a2, resolver: TypeResolver = default_resolver):
    e_type1 = a1.elementType
    e_type2 = a2.elementType

    if e_type1.typeName() == e_type2.typeName():
        if is_struct(e_type1):
            return ArrayType(merge_schemas(e_type1, e_type2, resolver))
        elif is_array(e_type1):
            return ArrayType(merge_array_schemas(e_type1, e_type2, resolver))
        else:
            return ArrayType(e_type1)
    else:
        return ArrayType(resolver.resolve(e_type1, e_type2))


def merge_schemas(s1, s2, resolver: TypeResolver = None):
    """
    Merge DataFrame schemas

    - merged schema will be nullable
    - For backward compatibility the schema of s1 has priority
    """
    if resolver is None:
        resolver = default_resolver

    s1_key_list = [f.name for f in s1]
    s2_key_list = [f.name for f in s2]
    s1_keys = set(s1_key_list)
    s2_keys = set(s2_key_list)
    final_order = s1_key_list + [key for key in s2_key_list if key not in s1_keys]

    errors = []
    new_fields = {}
    for key in s1_keys & s2_keys:
        s1_type = s1[key].dataType
        s2_type = s2[key].dataType
        try:
            if s1_type.typeName() == s2_type.typeName():
                if is_struct(s1_type):
                    new_fields[key] = merge_schemas(s1_type, s2_type, resolver)
                elif is_array(s1_type):
                    new_fields[key] = merge_array_schemas(s1_type, s2_type, resolver)
                else:
                    new_fields[key] = s1_type
            else:
                new_fields[key] = resolver.resolve(s1_type, s2_type)
        except TypeMergeError as e:
            errors.append(str(e))

    for key in s1_keys - s2_keys:
        new_fields[key] = s1[key].dataType
    for key in s2_keys - s1_keys:
        new_fields[key] = s2[key].dataType

    if errors:
        raise TypeMergeError("\n".join(errors))
    return StructType([StructField(key, new_fields[key], True) for key in final_order])
