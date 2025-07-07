from pyspark.sql.functions import col, when

def encode_column(df, column, mapping):
    expr = None
    for val, encoding in mapping.items():
        condition = (col(column) == val)
        expr = when(condition, encoding) if expr is None else expr.when(condition, encoding)
    expr = expr.otherwise(None)
    return df.withColumn(column, expr)
