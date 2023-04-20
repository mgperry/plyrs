import polars as pl
from polars.dataframe.groupby import GroupBy
from polars.lazyframe.groupby import LazyGroupBy
from typing import Sequence
import re

_mask = {
    "filter": "where",
}


def seq_or_args(args):
    if not args: return []

    if isinstance(args[0], Sequence):
        if len(args) > 1:
            raise ValueError("If first arg is a sequnece, no other args are allowed.")

        return list(args[0])

    return args


def as_col(x):
    return pl.col(x) if isinstance(x, str) else x

COLNAME = r'^col\("([^"]*)"\)$'

def col_name(x):
    if isinstance(x, str):
        return x

    m = re.match(COLNAME, str(x))
    if m:
        return m.group(1)


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
