import polars as pl
from polars.dataframe.groupby import GroupBy
from polars.lazyframe.groupby import LazyGroupBy
from collections.abc import Iterable

_mask = {
    "filter": "where",
}


def seq_or_args(args) -> list:
    if not args: return []

    if isinstance(args[0], Iterable):
        if len(args) > 1:
            raise ValueError("If first arg is a sequnece, no other args are allowed.")

        return list(args[0])

    return args


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
