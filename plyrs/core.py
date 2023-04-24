import polars as pl
from functools import reduce
from typing import Sequence

from . import column
from .decorators import wrap_polars, collector
from .utils import safe_collect, df_arg, _mask, chain, multi_col


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
def melt(df, id_vars=None, value_vars=None, variable_name=None, value_name=None):
    
    if id_vars is not None:
        id_vars = multi_col(id_vars)

    if value_vars is not None:
        value_vars = multi_col(value_vars)

    return df.melt(
        id_vars,
        value_vars,
        variable_name,
        value_name
    )


@wrap_polars
def rename(df, d={}, **kwargs):
    """
    plyrs wrapper for polars.DataFrame.rename

    Takes named arguments as well as a dict. dict keys and values must be strings.
    """

    return df.rename(d | kwargs)


@wrap_polars
def filter(df, *args):
    """
    plyrs wrapper for polars.DataFrame.filter

    Multple arguments are combined with '&'.
    """

    cond = reduce(lambda x, y: x & y, args)

    return df.filter(cond)


@wrap_polars
def drop(df, col, *more_cols):
    """
    plyrs wrapper for polars.DataFrame.drop.

    Takes multiple args as either strings or columns.
    """
    cols = [column.as_str(col) for col in chain(col, more_cols)]

    return df.drop(cols)


@wrap_polars
def drop_nulls(df, col, *more_cols):
    """
    plyrs wrapper for polars.DataFrame.drop_nulls.

    Takes multiple args as either strings or columns.
    """
    cols = [column.as_str(col) for col in chain(col, more_cols)]

    return df.drop_nulls(cols)


@wrap_polars
def get_column(df, col):
    """
    plyrs wrapper for polars.DataFrame.get_rolumn.

    Takesa single string or column.
    """
    col = column.as_str(col)

    return df.get_column(col)


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
    df2 = safe_collect(df2) if isinstance(df1, pl.DataFrame) else df2.lazy()
    
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
    "groupby",
    "with_columns",
    "agg",
    "sort",
    "lazy",
    "collect",
    "limit",
]

def _gen_method(method):
    """
    Generate plyrs bindings for common methods. 
    """

    def func(df, *args, **kwargs):
        return getattr(df, method)(*args, **kwargs)

    func.__name__ = method
    func.__qualname__ = method
    func.__doc__ = f"Wrapper for polars.DataFrame.{method}"
    
    return func


for m in _methods:
    func = _gen_method(m)
    func = wrap_polars(func)

    # popluate core.py namespace with generated functions
    globals()[m] = func

    # add aliases to namespace
    if m in _mask:
        globals()[_mask[m]] = func


_machinery = [
    "query",
    "wrap_polars",
    "collector",
]

_other_core = [
    "join",
    "pivot",
    "melt",
    "rename",
    "filter",
    "invoke",
    "drop",
    "drop_nulls",
    "get_column",
]

_core = _machinery + _methods + _other_core

__all__ = [_mask.get(f, f) for f in _core]
