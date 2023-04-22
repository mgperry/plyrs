from .utils import df_arg
import polars as pl
from functools import wraps

def wrap_polars(f):

    @wraps(f)
    def wrapper(*args, **kwargs):
        if df_arg(args):
            return f(*args, **kwargs)

        return lambda df: f(df, *args, **kwargs)
    
    return wrapper


def collector(f):
    """
    Coerce pl.LazyFrame to apply eager function and preserve state
    in return value.
    """

    @wraps(f)
    def new_func(df, *args, **kwargs):
        if isinstance(df, pl.LazyFrame):
            return f(df.collect(), *args, **kwargs).lazy()

        return f(df, *args, **kwargs)
    
    return new_func


