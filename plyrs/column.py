import polars as pl
import re

COLNAME = r'^col\("([^"]*)"\)$'

class Column:
    _aliases = [
        "all",
        "exclude",
    ]

    def __init__(self, redirect=True):
        self._redirect=redirect

    def __getattr__(self, s):
        if self._redirect and s in self._aliases:
            return getattr(pl, s)

        return self(s)

    def __call__(self, *s):
        return pl.col(*s)
    

def as_str(col):
    if col is None:
        return None

    if isinstance(col, str):
        return col

    m = re.match(COLNAME, str(col))
    if m:
        return m.group(1)


def as_col(col):
    if col is None:
        return None

    return pl.col(col) if isinstance(col, str) else col
