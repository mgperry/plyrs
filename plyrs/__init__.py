from .decorators import (
    wrap_polars,
    collector,
)

from .extra import(
    if_else,
    index,
    reorder,
)

from .utils import _mask

from .schema import schema

from . import core

# dplyr translations
_verbs = {
    "groupby":      "group_by",
    "with_columns": "mutate",
    "agg":          "summarise",
    "sort":         "arrange",
    "get_column":   "pull",
}


_core = core._core

for f in _core:
    name = _verbs.get(f, f)
    globals()[name] = getattr(core, f)

    if name in _mask:
        globals()[_mask[name]] = getattr(core, f)


_all = [
    "wrap_polars",
    "collector",
    "schema",
    "if_else",
    "reorder",
    "index",
]

_rename = {**_verbs, **_mask}

__all__ = [_rename.get(f, f) for f in _all + _core]
