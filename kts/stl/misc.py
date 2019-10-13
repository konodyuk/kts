import multiprocessing

import numpy as np
import pandas as pd

from kts.validation.leaderboard import leaderboard as lb
from kts.core.backend.memory import load
from kts.core.base_constructors import merge, wrap_stl_function, empty_like


def _apply_df(args):
    """

    Args:
      args:

    Returns:

    """
    df, func, num, kw = args
    return num, df.swifter.apply(func, **kw)


def apply(dataframe, function, **kwargs):
    """Applies function to dataframe faster.
    If n_threads is in kwargs and is greater than 1, applies by multiprocessing.
    :return: same as df.apply(function)

    Args:
      dataframe:
      function:
      **kwargs:

    Returns:

    """
    if "n_threads" in kwargs:
        n_threads = kwargs.pop("n_threads")
    else:
        n_threads = 1
    if n_threads == 1:
        return dataframe.swifter.apply(function, **kwargs)

    pool = multiprocessing.Pool(processes=n_threads)
    result = pool.map(
        _apply_df,
        [(d, function, i, kwargs)
         for i, d in enumerate(np.array_split(dataframe, n_threads))],
    )
    pool.close()
    result = sorted(result, key=lambda x: x[0])
    return pd.concat([i[1] for i in result])


def get_categorical(df):
    """

    Args:
      df:

    Returns:

    """
    cat_features = [col for col in df.columns if df[col].dtype == object]
    return cat_features


def get_numeric(df):
    """

    Args:
      df:

    Returns:

    """
    num_features = [col for col in df.columns if df[col].dtype != object]
    return num_features


def stack(ids):
    """

    Args:
      ids:

    Returns:

    """
    def __stack(df):
        oof_preds = merge([lb[id_exp].oof for id_exp in ids])
        oof_col_names = merge([lb[id_exp].oof.columns for id_exp in ids])
        try:
            res = oof_preds[df.index]
            if res.isna().sum().sum() > 0:
                bad_indices = res.isna().sum(axis=1) > 0
                preds = [lb[id_exp].predict(df[bad_indices]) for id_exp in ids]
                res[bad_indices] = merge([
                    pd.DataFrame(data=pred, columns=col_names)
                    for col_names, pred in zip(oof_col_names, preds)
                ])
        except:
            preds = [lb[id_exp].predict(df) for id_exp in ids]
            res = merge([
                pd.DataFrame(data=pred, columns=col_names)
                for col_names, pred in zip(oof_col_names, preds)
            ])
            res.set_index(df.index, inplace=True)
        return res

    return wrap_stl_function(stack, __stack)


def from_df(name):
    """

    Args:
      name:

    Returns:

    """
    def __from_df(df):
        tmp = load(name)
        return tmp.loc[df.index]

    return wrap_stl_function(from_df, __from_df)


def cv_apply(feature_constructor, split):
    """

    Args:
      feature_constructor:
      split:

    Returns:

    """
    raise NotImplementedError

    def __cv_apply(df):
        res = empty_like(df)