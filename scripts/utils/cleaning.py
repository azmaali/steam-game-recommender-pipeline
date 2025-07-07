from pyspark.sql.functions import col, trim, size
from functools import reduce
from pyspark.sql.types import StringType, ArrayType, MapType, StructType

def build_non_empty_condition(df, col_name, dtype):
    if isinstance(dtype, StringType):
        return col(col_name).isNotNull() & (trim(col(col_name)) != "")
    elif isinstance(dtype, ArrayType):
        return col(col_name).isNotNull() & (size(col(col_name)) > 0)
    elif isinstance(dtype, MapType):
        return col(col_name).isNotNull() & (size(col(col_name)) > 0)
    elif isinstance(dtype, StructType):
        return col(col_name).isNotNull()
    else:
        return col(col_name).isNotNull()

def filter_missing(df):
    conditions = [
        build_non_empty_condition(df, field.name, field.dataType)
        for field in df.schema.fields
    ]
    return df.where(reduce(lambda a, b: a & b, conditions))
