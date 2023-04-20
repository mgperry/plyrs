# plyrs

Why do we need another dplyr-in-pandas library?

We don't, really. The development of this library was driven by mostly aesthetic
considerations, interactive use, and a personal experiment in functional programming.

What you get:
- BRAND NEW data frame chaining syntax (see below).
- The best name
- Extend polars or modify default behaviour in a way that feels native
- Implemented as functions + decorators:
    - inputs and outputs are plain `polars` objects
    - mix in regular `polars` code without any problems.
    - easily roll your own, bypass `.pipe`
- affordances for lazy/eager DataFrames, with interactive use in mind (see Laziness below)
    - `dlpyrs` uses lazy dataframes wherever possible, without you noticing.
    - `@collector` decorator for functions which require eager evaluation.
- Optionally, dplyr verbs or style:
    - `agg`, really? `with_columns` is almost as bad.
    - `rename({"from": "to"})` has too many brackets and quotes
    - `.with_row_count()` starts from zero? Please, we are doing statistics
- Convenience functions like `separate` and `reorder`
- Full power of `polars` functions, this is a very thin wrapper not a re-implementation. 
- PLANNED: Wrap dataframe functions in types and create DSLs (like GRanges)

What you don't get:
There is no attempt to acheive feature parity with `dplyr` or reimplement
R syntax, that would be silly. The benefit of this is that you get the full power
of polars functions, which is pretty cool. It's also very easy to define similar
functions yourself (see How it Works) when you find pain points in polars, or you
have a common operation you want to reuse. 

A note:
I certainly *don't* think that polars should have been made like this, `plyrs` is very
much a 'porcelain' library and I don't see any good reason to use it in non-interactive
code (ie production or longer scripts). I personally spend a lot of time starting at analysis
code so maybe looks mean more to me than most. The same goes for including lots of small
convenience functions: it's a mistake in a serious library, but it can drastically
improve the interactive experience.

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

Polars is not shorter, but it does solve some of these issues:

```py
(flights_pl
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

BEHOLD! The dots are gone.

```py
query(
    flights_pl,
    filter(
        pl.col("year") == 2013,
        pl.col("month") == 1
    ),
    drop_nulls("arr_delay"),
    join(airlines_pl, on = "carrier", how = "left"),
    mutate(arr_delay = if_else(
        pl.col("arr_delay") > 0,
        pl.col("arr_delay"),
        0
    ))
    group_by(airline=pl.col("name")),
    summarise(
        flights = pl.count("airline"),
        mean_delay = pl.mean("arr_delay")
    ),
    arrange("mean_delay", reverse = True)
)
```

## How it works

The `query` function takes a data frame as a first argument, and a series
of functions. The verbs that you see in the code above are actually *factory functions*,
returning closures, which are then applied sequentially to the ipnut df. This works 
because polars *expressions* are actually values, and can therefore be assigned to
variables and passed between functions / contexts:

```py
jan_2013 = pl.col("year") == 2013 & pl.col("month") == 1
df.filter(jan_2013)
```

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

We can run the query with `query()`:

```py
query(df, *q)
```

Or call the functions manually:

```py
functools.reduce(lambda df, f: f(df), q, df).collect()
```

For the as-yet unenlightened among you, this is equivalent to calling the functions
manually in a loop:

```py
for f in q:
    df = f(df)

df.collect()
```

NB The final `collect()` statement can be skipped, this is just the default for
interactive use to get a meaningful result in the terminal:

```py
query(
    df,
    filter(pl.col("year") == 2015),
    ...,
    collect=False
)
```

A lot of the magic (if you can call it that) is done through the `@wrap_polars` decorator,
which takes any function defined on a dataframe:

```py
def reorder(df, cols):
    return df.select(cols, pl.all().exclude(cols))
```

and returns a generated function that looks like this:

```py
def reorder_factory(cols):
    return lambda df: df.select(cols, pl.all().exclude(cols))
```

which when called (e.g. `reorder(["id", "species"])`) gives roughly:

```py
def reorder_closure(df):
    return df.select(["id", "species"], pl.all().exclude(["id", "species"]))
```

Most code in `plyrs` (barring a few tricky cases) is fairly generic stuff which just passes
all {kw}args to a polars method (if you check the source most of the exported functions
are code-gen'd). This is a good thing, since it gives you all the
power of polars, rather than trying to second guess what you need and forcing you
to lobby the package writer if you want anything else.

However, aside from enabling the `query()` syntax and the functions defined in the package,
the `plyrs` machinery makes it easy to define your own functions and use them in the same
way as any other polars method: just add the
`@wrap_polars` decorator, and `@collector` if needed (as the first decorator) to a function
taking a dataframe as the first argument.

This means you can implement BUSINESS LOGIC(TM) mixed in with regular data manipulation:

```py
@wrap_polars
def active_orders(df):
    return df.filter(pl.col("order_shipped") == False)

@wrap_polars
def older_than(df, days):
    redaysurn df.filter(datetime.today() - pl.col("order_placed") >= timedelta(days=days))

query(
    df
    active_orders,
    older_than(7),
    group_by(employee=pl.col("assigned_to")),
    summarise(late_orders=pl.count()),
    mutate(status = if_else(pl.col("late_orders") > 5, "FIRED", "NEEDS_IMPROVEMENT")
)
```

NB This can be done with `.pipe`, sure, I refer you to the first paragraph. `plyrs`
doesn't use `.pipe` internally because it's not available everywhere, namely after
`.groupby` calls.

There is a another trick in the decorator: if the first argument to a function is a
dataframe, then it just calls the function directly. This means that everything can
be used outside of the `query` context:

```py
reorder(df, ["id", "species"])
```

# Namespaces

`plyrs` is designed with multiple namespaces, and with some support for (gasp!)
`import *` usage.

The full namespace `import plyrs` gets you:
- core machinery `[query, wrap_polars, ...]`
- all the polars wrapper functions, aliased to `dplyr` verbs (see below).
- extra functions, including some copied from `dplyr`

Alternatively, `import plyrs.core as ply` gets you:
- core machinery
- polars wrapper functions (non-aliased)

Both namespaces contain `where` as an alias to `filter`, which is omitted on `import *`
because it conflits with the builtin.

List of changes (possibly complete):
- `agg` => `summarise`
- `with_columns` => `mutate`
- `grouby` => `group_by`, I realise this is petty
- `get_column` => `pull`
- `rename` takes kwargs in addition to a single dict, kwargs have precedence.
- `plyrs` keeps `melt` and `pivot` even though this mixes various generations of
    tidyverse evolution, because they're the most obvious names.
- the renaming is implemented through a dict (`_verbs`) in package init.

some extras:
- `reorder` to change column order
- `separate` to splite delited columns
- `index` wraps `with_row_count`, starts from 1, defaults to "id" and can add a prefix
- `if_else` wrapping `pl.when().then().otherwise()`
- add your own!

# Laziness

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

# Dark Magic

I don't personally condone this level of witchcraft, but `dplyrs` also contains
a class whose instances allow you to access columns (like `pl.col`) as object
attributes, by overriding `__getattr__`.

This idea comes from [sibua](https://github.com/machow/siuba), so credit goes
to the author 'machow' if he came up with it.

You have to instantiate an object to do this, and additionally the class isn't
`__all__`.

This is deliberate because:
a. it means you have to deliberately do this, and
b. everyone will have a different symbol they want to use (personally I don't like "_", but each to their own)

You use it like this:

```py
from plyrs import *
from plyrs import ColAlias as make_alias

col = make_alias()

select(iris, col.species, col.sepal_length, col.sepal_width)

mutate(binomial="Iris " + col.species)
```

`.all` and `.exclude` are aliased. If you don't want this, pass `redirect=False`
to the constructor. Calling a ColAlias instance directly will pass any arguments
to `pl.col`; this can also be used to access a column called "all" ie `col("all")`.

```py
query(
    df,
    select(col.species, col("^sepal_.*$")),
    group_by(col.species),
    summarise(col.exclude("species").mean()),
    select(col(pl.Float64))
)
```

Functions like `pl.mean("sepal_width")` are not aliased, because these are just
sugar and `col.sepal_width.mean()` is easier to type, and reduces the chance of
collisions massively (e.g. `sum` is a very common columne name).

# Bonus

Similarly ergonomic plotnine bindings:

```py
from plyrs.plot import plot, geom, ggplot, theme, labels

base_plot = plot(
    ggplot(df, x="sepal_length", y="sepal_width", colour="species"),
    geom.point()
)

formatting = [theme.tufte(), labels.ggtitle("i <3 hadley")]

plot(
    base_plot,
    *formatting
)

```

- `plot` function combines arguments, no more R-style "+"
- Coerce polars dataframe and dictionary `aes()` arguments
- all functions inside a smaller namespace
- geoms, coords, scales etc. are namespaced with extra typing removed.
- easily compose lists of geoms
