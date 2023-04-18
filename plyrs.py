from functools import reduce
from types import SimpleNamespace

def query(df, *funcs, lazy=True, collect=True):
    if lazy:
        df = df.lazy()

    funcs = list(funcs)
    
    if collect:
        funcs.append(lambda df: df.collect())

    return reduce(lambda df, f: f(df), funcs, df)

def wrap_args(f_name):
    def wrapper(*args, **kwargs):
        return lambda df: getattr(df, f_name)(*args, **kwargs)
    
    return wrapper

def collector(wrapper):
    def collect_wrapper(*args, lazy=True, **kwargs):
        func = wrapper(*args, **kwargs)
        return lambda df: df.collect().pipe(func).lazy()
    
    return collect_wrapper

translations = {
    "select": "select",
    "drop": "drop",
    "filter": "filter",
    "group_by": "group_by",
    "mutate": "with_columns",
    "summarise": "agg",
    "pivot": "pivot",
    "melt": "melt",
}

nonlazy = ["pivot"]

dplyr = SimpleNamespace()

for name, method in translations.items():
    f = wrap_args(method)

    if method in nonlazy:
        f = collector(f)
    
    setattr(dplyr, name, f)
