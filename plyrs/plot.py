from plotnine import ggplot, aes
from functools import reduce
from typing import Union
from types import SimpleNamespace
import operator
import polars as pl
import pandas as pd

def plot(df: Union[pl.DataFrame, pd.DataFrame, None], mapping: Union[dict, aes, None], *args):
    """
    Wrapper of plotnine.ggplot to avoid "+". If you want to create
    a ggplot instance without data or mappings, these must be
    explicitly set to None, eg:

    plot(
        None,
        aes(x="width", y="height"),
        geom_point(data=my_df)
    )

    """
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    elif not isinstance(df, (pd.DataFrame, type(None))):
        raise ValueError("First argument 'df' must be dataframe or explicit None")
    
    if isinstance(mapping, dict):
        mapping = aes(**mapping)
    elif not isinstance(mapping, (aes, type(None))):
        raise ValueError("Second argument 'mapping' must be dict, aes() or explicit None")

    p = ggplot(df, mapping)
    return reduce(operator.add, args, p)

# import evereything into individual namespaces
def _import(mod, prefix):
    d = {}

    for obj in mod.__all__:
        name = obj.removeprefix(prefix)
        d[name] = getattr(mod, obj)

    return SimpleNamespace(**d)

from plotnine import mapping, guides, labels, qplot, watermark, ggsave, save_as_pdf_pages

import plotnine.geoms
geom = _import(plotnine.geoms, "geom_")

import plotnine.coords
coord = _import(plotnine.coords, "coord_")

import plotnine.facets
facet = _import(plotnine.facets, "facet_")

import plotnine.positions
position = _import(plotnine.positions, "position_")

import plotnine.scales
scale = _import(plotnine.scales, "scale_")

import plotnine.stats
stat = _import(plotnine.stats, "stat_")

import plotnine.themes
theme = _import(plotnine.themes, "theme_")
theme.five38 = plotnine.themes.theme_538
delattr(theme, "538")

__all__ = [
    "plot",
    "aes",
    "mapping",
    "guides",
    "qplot",
    "watermark",
    "ggsave",
    "save_as_pdf_pages",
    "geom",
    "coord",
    "facet",
    "labels",
    "position",
    "scale",
    "stat",
    "theme"
]
