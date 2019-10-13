import pandas as pd

from kts.core.dataframe import DataFrame as KTDF
import kts.core.feature_constructor
from kts.util.misc import extract_signature


def wrap_stl_function(outer_function, inner_function):
    """

    Args:
      outer_function:
      inner_function:

    Returns:

    """
    source = f"stl.{outer_function.__name__}({extract_signature(outer_function)})"
    fc = kts.core.feature_constructor.FeatureConstructor(inner_function, cache_default=False, stl=True)
    fc.source = source
    fc.args = extract_signature(outer_function, return_dict=True)
    return fc


def __empty_like(df):
    if isinstance(df, KTDF):
        return KTDF(df[[]].copy(), df.train, df.encoders, df.slice_id)
    else:
        return KTDF(df[[]].copy())


empty_like = kts.core.feature_constructor.FeatureConstructor(__empty_like, cache_default=False)
empty_like.stl = True
empty_like.source = "stl.empty_like"

identity = kts.core.feature_constructor.FeatureConstructor(lambda df: df, cache_default=False)
identity.stl = True
identity.source = "stl.identity"


def merge(dfs):
    """

    Args:
      dfs:

    Returns:

    """
    if len(dfs) == 1:
        return dfs[0]
    return pd.concat(dfs, axis=1)


def column_selector(columns):
    """

    Args:
      columns:

    Returns:

    """
    def __col_selector(df):
        return df[[col for col in columns if col in df.columns]]

    # return FeatureConstructor(__col_selector, cache_default=False)
    return wrap_stl_function(column_selector, __col_selector)


def column_dropper(columns):
    """

    Args:
      columns:

    Returns:

    """
    def __col_dropper(df):
        return df.drop([col for col in columns if col in df.columns], axis=1)

    # return FeatureConstructor(__col_dropper, cache_default=False)
    return wrap_stl_function(column_dropper, __col_dropper)


def concat(funcs):
    """

    Args:
      funcs:

    Returns:

    """
    funcs = sum(
        [
            i.args["funcs"] if i.source.startswith("stl.concat") else [i]
            for i in funcs
        ],
        [],
    )

    def __concat(df):
        return merge([func(df) for func in funcs])

    fc = wrap_stl_function(concat, __concat)
    fc.source = f"stl.concat([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    return fc


def compose(funcs):
    """

    Args:
      funcs:

    Returns:

    """
    if (len(funcs) == 2 and funcs[0].source.startswith("stl.concat")
            and funcs[1].source.startswith("stl.column_selector")):
        selection_list = funcs[1].args["columns"]
        return concat(
            [func + selection_list for func in funcs[0].args["funcs"]])

    def __compose(df):
        res = identity(df)
        for func in funcs:
            res = func(res)
        return res

    fc = wrap_stl_function(compose, __compose)
    fc.source = f"stl.compose([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    return fc