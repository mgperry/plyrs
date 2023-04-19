from polars.datatypes import dtype_to_py_type as to_py
from functools import wraps

def with_arguments(decorator):
    
    @wraps(decorator)
    def func(*args, **kwargs):
        return lambda f: decorator(f, *args, **kwargs)

    return func


def check(schema, dtypes, strict=False):
    print("inside check")
    errors = []

    for col, T in schema.items():
        dtype = to_py(dtypes[col]) if col in dtypes else None
        if dtype != T:
            errors.append(col)

    if errors: raise TypeError(f"df has incorrect or missing cols: {errors}")
    
    if strict and len(dtypes) > len(schema):
        raise TypeError("Extra columns present.")
        

@with_arguments
def schema(f, _check=True, _strict=False, **spec):

    @wraps(f)
    def new_func(df, *args, **kwargs):
        if _check:
            check(spec, df.schema, _strict)

        return f(df, *args, **kwargs)

    new_func._schema = spec

    return new_func
