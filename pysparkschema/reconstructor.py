from .error import ReconstructError
from .resolver import is_struct, is_array, is_same_type
import pyspark.sql.functions as F


def reconstruct(col, old_data_type, new_data_type):
    """
    Reconstruct column by key to be match to the new schema
    """

    if is_same_type(old_data_type, new_data_type):
        if is_struct(new_data_type):
            old_schema_dict = {
                f.name: old_data_type[f.name].dataType for f in old_data_type
            }
            new_schema_dict = {
                f.name: new_data_type[f.name].dataType for f in new_data_type
            }
            return F.struct(
                *[
                    reconstruct(col[key], old_schema_dict.get(key), new).alias(key)
                    if old_schema_dict.get(key)
                    else F.lit(None).cast(new).alias(key)
                    for key, new in new_schema_dict.items()
                ]
            )
        elif is_array(new_data_type):
            return F.transform(
                col,
                lambda x: reconstruct(
                    x, old_data_type.elementType, new_data_type.elementType
                ),
            )
        else:
            return col
    else:
        if is_struct(new_data_type):
            raise ReconstructError("Not struct type cannot convert to struct type")
        elif is_array(new_data_type):
            raise ReconstructError("Not array type cannot convert to array type")
        elif is_struct(old_data_type):
            raise ReconstructError("Struct type cannot convert to not struct type")
        elif is_array(old_data_type):
            raise ReconstructError("Array type cannot convert to not array type")
        else:
            return col.cast(new_data_type)
