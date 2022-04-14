from pyspark.sql.types import StructType, StructField, ArrayType


class TypeMergeError(RuntimeError):
    pass

def is_null(t):
    return t.typeName() in ["null", "void"]


def merge_array_schemas(a1, a2):
    a1_elem_typename = a1.elementType.typeName()
    a2_elem_typename = a2.elementType.typeName()

    if a1_elem_typename == a2_elem_typename:
        if a1_elem_typename == "struct":
            return ArrayType(merge_schemas(a1.elementType, a2.elementType))
        elif a1_elem_typename == "array":
            return ArrayType(merge_array_schemas(a1.elementType, a2.elementType))
        else:
            return ArrayType(a1.elementType)
    elif is_null(a1.elementType) and not is_null(a2.elementType):
        return ArrayType(a2.elementType)
    elif not is_null(a1.elementType) and is_null(a2.elementType):
        return ArrayType(a1.elementType)

    raise TypeMergeError(f"Schema {a1.elementType} and {a2.elementType} cannot be merged")


def merge_schemas(s1, s2):
    """
    Merge DataFrame schemas

    - merged schema will be nullable
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
                new_fields[key] = merge_schemas(s1[key].dataType, s2[key].dataType)
            elif s1_type == "array":
                try:
                    new_fields[key] = merge_array_schemas(
                        s1[key].dataType, s2[key].dataType
                    )
                except TypeMergeError as e:
                    errors.append(str(e))
            else:
                new_fields[key] = s1[key].dataType
        elif is_null(s1[key].dataType) and not is_null(s2[key].dataType):
            new_fields[key] = s2[key].dataType
        elif not is_null(s1[key].dataType) and is_null(s2[key].dataType):
            new_fields[key] = s1[key].dataType
        else:
            errors.append(f"Failed to merge schema {s1_type} != {s2_type}")

    for key in s1_keys - s2_keys:
        new_fields[key] = s1[key].dataType
    for key in s2_keys - s1_keys:
        new_fields[key] = s2[key].dataType

    if errors:
        raise TypeMergeError("\n".join(errors))
    return StructType([StructField(key, new_fields[key], True) for key in final_order])

