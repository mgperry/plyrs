import polars as pl
from functools import reduce
from typing import Sequence

from .decorators import wrap_polars, collector
from .utils import safe_collect, df_arg, _mask, col_name, seq_or_args


def query(df, *fs, collect=True):
    if collect:
        fs = fs + (safe_collect,)

    return reduce(lambda df, f: f(df), fs, df.lazy())


@wrap_polars
@collector
def pivot(df, *args, **kwargs):
    """
    plyrs wrapper for polars.DataFrame.pivot.

    If 'df' is lazy, will colect and then call .lazy() on result.
    """
    return df.pivot(*args, **kwargs)


@wrap_polars
def rename(df, d={}, **kwargs):
    """
    plyrs wrapper for polars.DataFrame.rename

    Takes named arguments as well as a dict.
    """

    d.update(kwargs)

    return df.rename(d)


@wrap_polars
def filter(df, *args):
    """
    plyrs wrapper for polars.DataFrame.filter

    Takes multiple args, passed to df.filter() as a list.
    """

    cond = reduce(lambda x, y: x & y, args)

    return df.filter(cond)


@wrap_polars
def drop_nulls(df, *args):
    """
    plyrs wrapper for polars.DataFrame.drop_nulls.

    Takes multiple args as either strings or columns.
    """

    args = [col_name(arg) for arg in seq_or_args(args)]

    return df.drop_nulls(args)


@wrap_polars
def invoke(df, method, *args, **kwargs):
    """
    Run polars any method using plyrs syntax.
    """
    return getattr(df, method)(*args, **kwargs)


def _join(df1, df2, *args, **kwargs):
    """
    helper for lazy dataframes in plyrs.core.join
    """
    if isinstance(df1, pl.DataFrame):
        df2 = safe_collect(df2)
    else:
        df2 = df2.lazy() # safe

    return df1.join(df2, *args, **kwargs)


def join(*args, **kwargs):
    """
    plyrs wrapper for polars.DataFrame.join

    Dispatches on first 2 arguments istead of one.
    Coerces the second data frame to lazy or eager to match the first.
    """
    if df_arg(args) and df_arg(args, 1):
        return _join(*args, **kwargs)
    else:
        return lambda df: _join(df, *args, **kwargs)


_methods = [
    "select",
    "drop",
    "groupby",
    "with_columns",
    "agg",
    "melt",
    "sort",
    "lazy",
    "collect",
    "get_column",
    "limit",
]

def _gen_method(method):
    """
    Generate plyrs bindings for common methods. 
    """

    def func(df, *args, **kwargs):
        return getattr(df, method)(*args, **kwargs)
    
    func.__name__ = m
    func.__qualname__ = m
    func.__doc__ = f"Wrapper for polars.DataFrame.{m}"
    
    return func


for m in _methods:
    func = _gen_method(m)
    func = wrap_polars(func)

    # popluate core.py namespace with generated functions
    globals()[m] = func

    # add aliases to namespace
    if m in _mask:
        globals()[_mask[m]] = func


_other_core = [
    "query",
    "join",
    "pivot",
    "rename",
    "filter",
    "invoke",
    "drop_nulls",
]

_core = _methods + _other_core

__all__ = [_mask.get(f, f) for f in _core]
