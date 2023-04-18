# plyrs

Why do we need another dplyr-in-pandas library?

We don't, really. The development of this library was driven by mostly aesthetic considerations, interactive use, and as an experiment in functional programming.

What you get:
- BRAND NEW data frame chaining syntax (see below).
- Implemented as functions + decorators:
    - inputs and outputs are plain `polars` objects
    - mix in regular `polars` code without any problems.
    - easily roll your own, bypass `.pipe`
- Some minor affordances around lazy/eager DataFrames, with interactive use in mind
- Optionally, dplyr verbs or style where it makes sense:
    - `agg`, really? `with_columns` is almost as bad.
    - `rename({"from": "to"})` has way too many brackets and quotes
- Convenience functions like `separate` and `reorder`
- Full power of `polars` functions, this is a very this wrapper not a re-implementation. 

What you don't get:
There is no attempt to acheive feature parity with `dplyr` or reimplement
R syntax, that would be silly. The benefit of this is that you get the full power
of polars functions, which is pretty cool. It's also very easy to define similar
functions yourself (see Extension) when you find pain points in polars, or you
have a common operation you want to reuse. 

A note:
I certainly *don't* think that polars should have been done like this,


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
query(flights_pl,
    filter(
        pl.col("year") == 2013,
        pl.col("month") == 1
    ),
    drop_nulls("arr_delay"),
    join(airlines_pl, on = "carrier", how = "left"),
    mutate(
        arr_delay = if_else(
            pl.col("arr_delay") > 0,
            pl.col("arr_delay"),
            0
        )
        airline = pl.col("name")
    )
    groupby("airline"),
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
that return functions which are sequentially applied to the data frame. This works 
because polars *expressions* don't need to be attached to a dataframe instance, so can
be built up in isolation.

This clearer when we write the code like this:

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

query(df, *q)
```

A lot of the 'magic' is done through the `@wrap_polars` decorator, which takes a
function that looks like this, written very naturally:

```py
@wrap_polars
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

Of course, we could write code like this but it's not how most people's 
minds work. The decorator also adds a dispatch mechanism, so that if the
first argument looks like a polars dataframe then it just calls the function, so we can use the same function like this:

```py
reorder(df, ["id", "species"])
```

Many functions are much more generic, which lets you use the full
power of polars. For example:

```py
def select(df, *args, **kwargs):
    return df.select(*args, **kwargs)
```

In fact, most methods can be converted just using a factory function and `getattr`.

# Laziness

`query()` aims to work mostly with lazy functions. Any Dataframe passed to `query` is
converted to lazy at the start, and the code should run without manualy calls to `collect`
and `lazy`. The few functions which require collection are wrapped
in `@collector`, which will collect the DF as needed, but re-lazify the output again
if the initial dataframe was lazy. `query()` collects automatically, aligning with
the goal of interactive use, but this can be disabled (`lazy=False`). `join` is a special
case, which will coerce the `right` dataframe to the same style as `left`.

# Bonus

non-insane plotnine bindings:

```py
plot(
    iris,
    dict(x="sepal_length", y="sepal_width", colour="species"),
    geom_point(),
    ...
)
```
