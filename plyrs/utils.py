import polars as pl
from polars.dataframe.groupby import GroupBy
from polars.lazyframe.groupby import LazyGroupBy


_mask = {
    "filter": "where",
}


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
