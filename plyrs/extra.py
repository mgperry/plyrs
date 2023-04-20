import polars as pl
from .decorators import wrap_polars
from .utils import as_col, col_name


def if_else(when, then, otherwise):
    """
    Functional wrapper of pl.when.then.otherwise
    """
    return pl.when(when).then(then).otherwise(otherwise)


@wrap_polars
def index(df, name="id", start=1, prefix: str=None):
    """
    Adds an index column to the dataframe. Wraps `with_row_columns` but starts
    from 1, default to "id" and optionally adds a prefix.
    """
    df = df.with_row_count(name).with_columns(pl.col(name) + start)
    if prefix:
        df = df.with_columns((prefix + pl.col("id").cast(str)).keep_name())
    return df


@wrap_polars
def reorder(df, cols):
    """
    Reorder dataframe columns.
    """
    return df.select(cols, pl.all().exclude(cols))


@wrap_polars
def separate(df, col, into, sep, keep=False):
    alias = "temp" if keep else col_name(col)
    n = len(into) - 1
    exp = as_col(col).str.split_exact(sep, n).alias(alias).struct.rename_fields(into)
    return df.with_columns(exp).unnest(alias)


_extra = [
    "if_else",
    "index",
    "reorder",
    "separate",
]


__all__= _extra
