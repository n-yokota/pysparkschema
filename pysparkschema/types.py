from pyspark.sql.types import StructType, StructField, ArrayType
from .resolver import (
    TypeResolver,
    NullTypeResolveStrategy,
    NumberTypeResolveStrategy,
)
from .error import TypeMergeError


default_resolver = TypeResolver(
    [
        NullTypeResolveStrategy,
        NumberTypeResolveStrategy,
    ]
)


def merge_array_schemas(a1, a2, resolver: TypeResolver = default_resolver):
    a1_elem_typename = a1.elementType.typeName()
    a2_elem_typename = a2.elementType.typeName()

    if a1_elem_typename == a2_elem_typename:
        if a1_elem_typename == "struct":
            return ArrayType(merge_schemas(a1.elementType, a2.elementType))
        elif a1_elem_typename == "array":
            return ArrayType(merge_array_schemas(a1.elementType, a2.elementType))
        else:
            return ArrayType(a1.elementType)
    else:
        new_type = resolver.resolve(a1.elementType, a2.elementType)
        return ArrayType(new_type)


def merge_schemas(s1, s2, resolver: TypeResolver = default_resolver):
    """
    Merge DataFrame schemas

    - merged schema will be nullable
    - For backward compatibility the schema of s1 has priority
    """

    s1_key_list = [f.name for f in s1]
    s2_key_list = [f.name for f in s2]
    s1_keys = set(s1_key_list)
    s2_keys = set(s2_key_list)
    final_order = s1_key_list + [key for key in s2_key_list if key not in s1_keys]

    errors = []
    new_fields = {}
    for key in s1_keys & s2_keys:
        s1_type = s1[key].dataType.typeName()
        s2_type = s2[key].dataType.typeName()
        if s1_type == s2_type:
            if s1_type == "struct":
                new_fields[key] = merge_schemas(
                    s1[key].dataType, s2[key].dataType, resolver
                )
            elif s1_type == "array":
                try:
                    new_fields[key] = merge_array_schemas(
                        s1[key].dataType, s2[key].dataType, resolver
                    )
                except TypeMergeError as e:
                    errors.append(str(e))
            else:
                new_fields[key] = s1[key].dataType
        else:
            try:
                new_fields[key] = resolver.resolve(s1[key].dataType, s2[key].dataType)
            except TypeMergeError as e:
                errors.append(str(e))

    for key in s1_keys - s2_keys:
        new_fields[key] = s1[key].dataType
    for key in s2_keys - s1_keys:
        new_fields[key] = s2[key].dataType

    if errors:
        raise TypeMergeError("\n".join(errors))
    return StructType([StructField(key, new_fields[key], True) for key in final_order])
