import plotnine
from plotnine import aes

import polars as pl
import pandas as pd

from functools import reduce
from typing import Union
from types import SimpleNamespace
import operator


def ggplot(
        df: Union[pl.DataFrame, pd.DataFrame],
        mapping: Union[aes, dict] = {},
        **kwargs
    ):
    """
    Wrapper of the ggplot function in plotnine.

    Polars dataframe are auto-converted (inc LazyFrames which
    are collected).

    Creating a plot without data requires an explicit None
    argument:

    `ggplot(None, aes("var1", "var2"))`

    mapping (ie aes) is specified as an aes object or dict:

    ```
    ggplot(df, {"x": "var1", "y": "var2"})
    ggplot(df, aes("var1", "var2"))
    ```

    kwargs are added to aes (if present), can be mixed if desired, and
    kwargs takes precedence:

    ```
    ggplot(df, x="var1", y="var2")

    default_aes = {"x": "ignored", "y": "var2"}
    ggplplot(df, default_aes, x="var1", colour="var3")
    ```

    If needed, 'environment' argument is pop'd from kwargs. This is not
    a valid aesthetic mapping collisions should not be an issue.
    """

    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    elif isinstance(df, pl.LazyFrame):
        df = df.collect().to_pandas()
    elif not isinstance(df, (pd.DataFrame, type(None))):
        raise ValueError("df argument must be a dataframe or None")
    
    env = kwargs.pop("environment", None)
    
    mapping = aes(**dict(mapping)) # handles aes/dict case
    mapping.update(kwargs)

    return plotnine.ggplot(df, mapping, env)

def plot(p, *args):
    """
    Wrapper of plotnine to avoid (p + geom + ...) syntax. First argument must be a plot, ie
    a call to ggplot() or a previously existing plot.
    
    ```
    p = plot(
        ggplot(iris, x="sepal_length", y="sepal_width", colour="species"),
        geom_point()
    )
    ```
    """

    return reduce(operator.add, args, p)

# import evereything into individual namespaces
def _import(mod, prefix):
    d = {}

    for obj in mod.__all__:
        name = obj.removeprefix(prefix)
        d[name] = getattr(mod, obj)

    return SimpleNamespace(**d)

from plotnine import (
    mapping,
    guides,
    labels,
    qplot,
    watermark,
    ggsave,
    save_as_pdf_pages
)

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
    "ggplot",
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
