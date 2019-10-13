import copy

import pandas as pd
from sklearn.preprocessing import StandardScaler

from kts.core.base_constructors import empty_like, wrap_stl_function
from kts.stl.misc import get_numeric


def discretize(cols, bins, prefix="disc_"):
    """

    Args:
      cols:
      bins:
      prefix:  (Default value = 'disc_')

    Returns:

    """
    def __discretize(df):
        res = empty_like(df)
        for col in cols:
            if df.train:
                res[prefix + str(bins) + "_" + col], enc = pd.cut(df[col],
                                                                  bins,
                                                                  retbins=True)
                df.encoders[f"__disc_{bins}_{col}"] = enc
            else:
                enc = df.encoders[f"__disc_{bins}_{col}"]
                res[prefix + str(bins) + "_" + col] = pd.cut(df[col], enc)
        return res

    return wrap_stl_function(discretize, __discretize)


def discretize_quantile(cols, bins, prefix="disc_q_"):
    """

    Args:
      cols:
      bins:
      prefix:  (Default value = 'disc_q_')

    Returns:

    """
    def __discretize_quantile(df):
        res = empty_like(df)
        for col in cols:
            if df.train:
                res[prefix + str(bins) + "_" + col], enc = pd.qcut(
                    df[col], bins, retbins=True)
                df.encoders[f"__disc_q_{bins}_{col}"] = enc
            else:
                enc = df.encoders[f"__disc_q_{bins}_{col}"]
                res[prefix + str(bins) + "_" + col] = pd.cut(df[col], enc)
        return res

    return wrap_stl_function(discretize_quantile, __discretize_quantile)


def standardize(cols=None, prefix="std_"):
    """

    Args:
      cols:  (Default value = None)
      prefix:  (Default value = 'std_')

    Returns:

    """
    def __standardize(df):
        res = empty_like(df)
        cols_inner = copy.copy(cols)
        if cols_inner is None:
            cols_inner = get_numeric(df)
        for col in cols_inner:
            if df.train:
                encoder = StandardScaler()
                res[prefix + col] = encoder.fit_transform(df[[col]].values)
                df.encoders[f"__std_{col}"] = encoder
            else:
                encoder = df.encoders[f"__std_{col}"]
                res[prefix + col] = encoder.transform(df[[col]].values)
        return res

    return wrap_stl_function(standardize, __standardize)