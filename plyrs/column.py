import polars as pl


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
