import polars as pl
from polars.dataframe.groupby import GroupBy
from polars.lazyframe.groupby import LazyGroupBy

from typing import Sequence, Union

from . import column

_mask = {
    "filter": "where",
}


def chain(cols, more_cols):
    if isinstance(cols, (str, pl.Expr)):
        cols = [cols]
    
    return cols + list(more_cols)


def multi_col(cols: Union[str, Sequence]) -> list[str]:
    """
    Coerce arguments taking a string or list of strings, and allow column arguments also.
    """
    if isinstance(cols, (str, pl.Expr)):
        cols = [cols]

    return [column.as_str(col) for col in cols]


def safe_collect(x):
    return x.collect() if isinstance(x, pl.LazyFrame) else x


df_like = (
    pl.DataFrame,
    pl.LazyFrame,
    GroupBy,
    LazyGroupBy
)


def df_arg(args, i=0):
    return len(args) > i and isinstance(args[i], df_like)
