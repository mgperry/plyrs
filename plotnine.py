from plotnine import ggplot, aes
from typing import Any
from functools import reduce
import operator

def plot(df, map: dict[str, Any], *args):
    p = ggplot(df, aes(**map))
    return reduce(operator.add, args, p)

