from functools import reduce
from types import SimpleNamespace
import polars as pl
from polars.dataframe.groupby import GroupBy
from polars.lazyframe.groupby import LazyGroupBy

def safe_collect(df):
    return df.collect() if isinstance(df, pl.LazyFrame) else df


df_like = (
    pl.DataFrame,
    pl.LazyFrame,
    GroupBy,
    LazyGroupBy
)


def df_first(args):
    return args and isinstance(args[0], df_like)


def df_second(args):
    return len(args) >= 2 and isinstance(args[1], df_like)


def wrap_polars(f):
    def wrapper(*args, **kwargs):
        if df_first(args):
            return f(*args, **kwargs)
        else:
            return lambda df: f(df, *args, **kwargs)
    
    return wrapper


def wrap_method(method):
    """
    Get a function which call a method on a polars.DataFrame.
    """
    def func(df, *args, **kwargs):
        return getattr(df, method)(*args, **kwargs)
    
    return func


def collector(f):
    """
    Coerce pl.LazyFrame to apply eager function and preserve state
    in return value.
    """
    def new_func(df, *args, **kwargs):       
        if isinstance(df, pl.LazyFrame):
            return f(df.collect(), *args, **kwargs).lazy()
        else:
            return f(df, *args, **kwargs)
    
    return new_func


def query(df, *fs, collect=True):   
    if collect:
        fs = fs + (safe_collect,)

    return reduce(lambda df, f: f(df), fs, df.lazy())


translations = {
    "select":     "select",
    "drop":       "drop",
    "filter":     "filter",
    "group_by":   "groupby",
    "mutate":     "with_columns",
    "summarise":  "agg",
    "pivot":      "pivot",
    "melt":       "melt",
    "arrange":    "sort",
    "lazy":       "lazy",
    "collect":    "collect",
    "drop_nulls": "drop_nulls",
}


nonlazy = ["pivot"]

dplyr = SimpleNamespace()
core = SimpleNamespace()

for verb, method in translations.items():
    func = wrap_method(method)
    func = collector(func) if method in nonlazy else func
    func = wrap_polars(func)

    setattr(dplyr, verb, func)
    setattr(core, method, func)


@wrap_polars
def rename(df, **kwargs):
    return df.rename(kwargs)

dplyr.rename = rename

@wrap_polars
def index(df, name="id", start=1, prefix: str=None):
    df = df.with_row_count(name).with_columns(pl.col(name) + start)
    if prefix:
        df = df.with_columns((prefix + pl.col("id").cast(str)).keep_name())
    return df

dplyr.index = index

@wrap_polars
def reorder(df, cols):
    return df.select(cols, pl.all().exclude(cols))

dplyr.reorder = reorder

@wrap_polars
def invoke(df, method, *args, **kwargs):
    """
    Run polars methods we might have missed using the same syntax.
    """
    return getattr(df, method)(*args, **kwargs)

dplyr.invoke = invoke


# deal with laziness based on df1
def join_inner(df1, df2, *args, **kwargs):
    if isinstance(df1, pl.DataFrame):
        df2 = safe_collect(df2)
    else:
        df2 = df2.lazy() # safe

    return df1.join(df2, *args, **kwargs)


# cannot dispatch on only first argument
def join(*args, **kwargs):
    if df_first(args) and df_second(args):
        return join_inner(*args, **kwargs)
    else:
        return lambda df: join_inner(df, *args, **kwargs)


dplyr.join = join
core.join = join

# non-dataframe functions
def if_else(when, then, otherwise):
    return pl.when(when).then(then).otherwise(otherwise)
