from pysparkschema.resolver import is_struct, is_array, is_string
import pyspark.sql.functions as F
from pyspark.sql import Row


def mask_field(col, col_name, col_data_type, mask_field_names, mask_func):
    """
    mask field value if field name is matched to mask_fields
    if you want to mask string element of array, you can include {col_name}.elem into mask_fields.
    """
    if col is None:
        return None
    if is_struct(col_data_type):
        return Row(
            **{
                k: mask_field(
                    v,
                    f"{col_name}.{k}",
                    col_data_type[k].dataType,
                    mask_field_names,
                    mask_func,
                )
                for k, v in col.asDict().items()
            }
        )
    elif is_array(col_data_type):
        return [
            mask_field(
                f,
                f"{col_name}.elem",
                col_data_type.elementType,
                mask_field_names,
                mask_func,
            )
            for f in col
        ]
    elif is_string(col_data_type):
        if col_name in mask_field_names:
            return mask_func(col)
    return col


def mask(df, mask_field_names, mask_func):
    """
    mask_field_names: list[nested field names] like ["a.b", c.elem.d"]
        you can set element of array with `.elem`
    mask_func: str -> str
    """
    for col in df.schema:
        col_data_type = df.schema[col.name].dataType
        df = df.withColumn(
            col.name,
            F.udf(
                lambda x: mask_field(
                    x, col.name, col_data_type, mask_field_names, mask_func
                ),
                col_data_type,
            )(col.name),
        )

    return df
