from polars.datatypes import dtype_to_py_type as to_py

def check_df_schema(dtypes, schema, strict=False):
    errors = []

    for col, T in schema.items():
        dtype = to_py(dtypes["col"]) if col in dtypes else None
        if dtype != T:
            errors.append(col)

    if errors:
        raise TypeError(
            f"Columns {errors} are missing for contain incorrect types."
        )
    
    extra_cols = [col for col in dtypes if not col in schema]

    if strict and extra_cols:
        raise TypeError(
            f"Strict mode enabled, dataframe contains extra columns {extra_cols}"
        )


def schema(_check=True, _strict=False, **spec):

    def decorator(f):
        if not _check: return f

        def new_func(df, *args, **kwargs):
            if _check:
                check_df_schema(df.schema, spec, _strict)

            return f(df, *args, **kwargs)

        return new_func
    
    return decorator


