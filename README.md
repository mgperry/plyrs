# plyrs

Why do we need another dplyr-in-pandas library?

We don't, really. The development of this library was driven by mostly aesthetic
considerations, interactive use, and a personal experiment in functional programming.

What you get (aside from the great name):
- BRAND NEW data frame chaining syntax (see below)
- Extend polars or modify default behaviour in a way that feels native
- Implemented transparently as functions + decorators:
    - uses regular `polars` objects
    - full power of `polars.Expr` in most contexts, mix in standard polars code when needed.
- uses lazy dataframes wherever possible, without you noticing (see Laziness)
    - optimised for interaction, `@collector` decorator for eager functions
- \[OPTIONAL\] Dplyr verbs (e.g. `mutate` vs. `with_columns`, `summarise` vs `agg`)
- Additional convenience functions e.g. `separate`
- `col` helper to reference columns without quotes.
    - `plyrs` functions should always accept this, even when polars required strings
- @schema decorator to validate inputs to domain-specific functions.

What you don't get:
There is no attempt to acheive feature parity with `dplyr` or reimplement
R syntax, that would be silly. The benefit of this is that you get the full power
of polars functions, which is pretty cool. It's also very easy to define similar
functions yourself (see How it Works) when you find pain points in polars, or you
have a common operation you want to reuse. 

A note:
I certainly *don't* think that polars should have been made like this, `plyrs` is very
much a 'porcelain' library probably shouldn't be used outside of interactive sessions
and short scripts (outside of @schema perhaps). I personally spend a lot of time
starting at analysis code, so maybe aesthetics mean more to me than most. The same goes
for including lots of small convenience functions: it's a mistake in a serious library,
but it can significantly improve the interactive experience.

## syntax

The accetped data-frame chaining syntax in python is like so:

```py
(flights_pd
  .query("year == 2013 & month == 1 & arr_delay.notnull()")
  .assign(arr_delay = flights_pd.arr_delay.clip(lower = 0))
  .merge(airlines_pd, how = "left", on = "carrier")
  .rename(columns = {"name": "airline"})
  .groupby("airline")
  .agg(flights = ("airline", "count"), mean_delay = ("arr_delay", "mean"))
  .sort_values(by = "mean_delay", ascending = False))
```

It's worth noting the complications using `.query` strings, and having to 
manually reference collumns in `.assign`, and the strings-as-functios in `.agg`.

Polars is not shorter, but it does solve some of these issues. A lot of the improvement
comes from the polars expression construct, which effectively delays its own evaluation
in order to reference columns in the data frame.

```py
(flights
  .filter((pl.col("year") == 2013) & (pl.col("month") == 1))
  .drop_nulls("arr_delay")
  .join(airlines_pl, on = "carrier", how = "left")
  .with_columns(
    [
      pl.when(pl.col("arr_delay") > 0)
        .then(pl.col("arr_delay"))
        .otherwise(0)
        .alias("arr_delay"),
      pl.col("name").alias("airline")
    ]
  )
  .groupby("airline")
  .agg(
    [
      pl.count("airline").alias("flights"),
      pl.mean("arr_delay").alias("mean_delay")
    ]
  )
  .sort("mean_delay", reverse = True)
)
```

BEHOLD! The dots are gone, along with the brackets and most of the quotes:

```py
query(
    flights,
    where(
        col.year == 2013,
        col.month == 1
    ),
    drop_nulls(col.arr_delay),
    join(airlines_pl, on = col.carrier, how = "left"),
    mutate(arr_delay = if_else(
        col.arr_delay > 0,
        col.arr_delay,
        0
    )),
    rename(name="airline"),
    group_by(col.airline),
    summarise(
        flights = col.airline.count(),
        mean_delay = col.arr_delay.mean()
    ),
    arrange(col.mean_delay, descending = True)
)
```

## How it works

`plyrs` uses polars `expr`s to capture expressions without evaluation, then applies
these expressions (through normal polars methods) to the original dataframe. What you
see in the code is not strictly a pipe: the extra arguments to `query` are in fact
functions which are sequentially applied to the input df (ie `flights`). The `plyrs`
verbs you see (`select`, `mutate` etc.) are actually factory functioons that create
closures, which are then passed to `query`.

This clearer when we create the lits of functions manually:

```py
q = [
    filter(
        pl.col("year") == 2013,
        pl.col("month") == 1
    ),
    drop_nulls("arr_delay"),
    join(airlines_pl, on = "carrier", how = "left"),
    ...
]
```

Then run the query with `query()`, or by calling the functions explicitly:

```py
query(df, *q) # collects by default

# equivalent to:
df.lazy()
for f in q:
    df = f(df)

df.collect()
```

For the big-brained:

```py
functools.reduce(lambda df, f: f(df), q, df).collect()
```

The magic happens in the `@wrap_polars` decorator, which takes any function defined on 
dataframe, and returns a factory function that will work in `query`.

In code:

```py
@wrap_polars
def reorder(df, cols):
    return df.select(cols, pl.all().exclude(cols))

# decorated function is now a factory
def reorder(cols):
    return lambda df: df.select(cols, pl.all().exclude(cols))
```

which when called (e.g. `reorder(["id", "species"])`) gives roughly:

```py
closure = lambda df: df.select(["id", "species"], pl.all().exclude(["id", "species"]))
```

This closure is what is passed to `query()`. 

Most code in `plyrs` (barring a few tricky cases) is fairly generic stuff which just
passes all \[kw\]args to a polars method: in fact, if you check the source most of the
exported functions are auto-generated from a list of method names. This gives you all
the power of polars rather than trying to second guess what you need.

There is a another trick in the decorator: if the first argument to a function is a
dataframe, then it just calls the function directly. This means that everything can
be used outside of the `query` context:

```py
reorder(df, ["id", "species"])
```

## Namespaces

`plyrs` is designed with multiple namespaces, and with some support for (gasp!)
`import *` usage.

The full namespace `import plyrs as ply` gets you:
- core machinery `[query, wrap_polars, ...]`
- all the polars wrapper functions, aliased to `dplyr` verbs (see below).
- extra functions, including some copied from `dplyr`
- `col`

Alternatively, `import plyrs.core as ply` gets you:
- core machinery
- polars wrapper functions (non-aliased)

Both namespaces contain `where` as an alias to `filter`, which is omitted on `import *`
because it conflits with the builtin.

List of name changes (possibly complete):
- `agg` => `summarise`
- `with_columns` => `mutate`
- `grouby` => `group_by`, I realise this is petty
- `get_column` => `pull`
- `rename` takes kwargs in addition to a single dict, kwargs have precedence.
- `plyrs` keeps `melt` and `pivot` even though this mixes various generations of
    tidyverse evolution, because they're the most obvious names.
- `drop_nulls` can take column arguments (also multiple arguments).

NB the renaming is implemented through a dict (`_verbs`) in package init.

some extras:
- `reorder` to change column order
- `separate` to split delited columns
- `index` wraps `with_row_count`, starts from 1, defaults to "id" and can add a prefix
- `if_else` wrapping `pl.when().then().otherwise()`
- add your own!

## Laziness

In general, the aim of `plyrs` is to get maximum laziness without any input from the user:
- `query()` works with lazy dataframes where possible.
    - `df.lazy()` is called before any function is run
    - `plyrs` functions always return lazy output from lazy input
    - therefore anything inside `query()` will be lazy without an explicit `collect()`
      call or user-defined functions.
- `query()` collects the final result by default
    - this aligns with the goal of interactive use
    - use `collect=False` to prevent this.
- functions which require collection (currently just `pivot` in `plyrs`) are wrapped in `@collector`
    - this calls `collect()` when needed, as the name suggests
    - `lazy()` is called on the output if the input was also lazy
        - this ensures interactive use outside `query` will generally show output
- `join` is a special case, which will coerce the `right` dataframe to the same style as `left`.
    - inside `query()` this will usually be lazy.

## Dark Magic

`dplyrs` also contains a class (`Column`) which allows you to access columns (like
`pl.col`) as object attributes, by overriding `__getattr__`, an idea taken from
[sibua](https://github.com/machow/siuba).

You use it like this:

```py
from plyrs import query, col, select, group_by, summarise, select

query(
    df,
    select(col.species, col("^sepal_.*$")),
    group_by(col.species),
    summarise(col.exclude("species").mean()),
    select(col(pl.Float64))
)
```

Calling `col` directly dispatches to `pl.col`, giving you multiple columns, regex and
type selections in the same way. `.all` and `.exclude` are aliasedto polars functions,
disable this with `col=Column(False)`. Many other functions are available via the expr
construct, e.g. `col.count.sum()`.

## Bonus

Similarly ergonomic plotnine bindings:

```py
from plotnine import ggplot, geom, stat, facet, theme, labels
from plotnine.data import mtcars

base_plot = plot(
    ggplot(mtcars, x='wt', y/'mpg', color='factor(gear)'),
    geom.point(),
    stat.smooth(method='lm'),
    facet.wrap('~gear')
)

formatting = [theme.tufte(), labels.ggtitle("i <3 hadley")]

plot(
    base_plot,
    *formatting
)

```

- `plot` function combines arguments, no more R-style "+" wrapper in brackets
- Coerce polars dataframe and dictionary `aes()` arguments
- geoms, coords, scales etc. are namespaced with extra typing removed.
- easily compose lists of geoms
