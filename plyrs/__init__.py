from .utils import _mask
from .schema import schema
from .column import Column

from . import core

from .extra import *
from .extra import _extra

_all = [
    "schema",
    "col",
]

_verbs = {
    "groupby":      "group_by",
    "with_columns": "mutate",
    "agg":          "summarise",
    "sort":         "arrange",
    "get_column":   "pull",
}

col = Column()

for f in core._core:
    name = _verbs.get(f, f)
    globals()[name] = getattr(core, f)

    if name in _mask:
        alias = _mask[name]
        globals()[alias] = getattr(core, f)
        _all.append(alias)
    else:
        _all.append(name)


__all__ = _all + _extra
