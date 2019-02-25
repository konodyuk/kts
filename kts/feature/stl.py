from .storage import FeatureConstructor
import pandas as pd
import numpy as np
from ..storage.dataframe import DataFrame as KTDF


def empty_like(df):
    if isinstance(df, KTDF):
        return KTDF(df[[]].copy(), df.train, df.encoders, df.slice_id)
    else:
        return KTDF(df[[]].copy())


identity = FeatureConstructor(lambda df: df, cache_default=False)
identity.stl = True
identity.source = "stl.identity"


def merge(dfs):
    if len(dfs) == 1:
        return dfs[0]
    return pd.concat([df.df if isinstance(df, KTDF) else df for df in dfs], axis=1)


def column_selector(columns):
    def __col_selector(df):
        return df[columns]

    return FeatureConstructor(__col_selector, cache_default=False)


def column_dropper(columns):
    def __col_dropper(df):
        return df.drop(columns, axis=1)

    return FeatureConstructor(__col_dropper, cache_default=False)


def concat(funcs):
    def __concat(df):
        return merge([func(df) for func in funcs])

    fc = FeatureConstructor(__concat, cache_default=False)
    fc.source = f"stl.concat([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    fc.__name__ = f"concat_{''.join([func.__name__[:2] if not func.stl else func.__name__ for func in funcs])}"
    fc.stl = True
    return fc


def compose(funcs):
    def __compose(df):
        res = empty_like(df)
        for func in funcs:
            res = merge([res, func(res)])
        return res

    fc = FeatureConstructor(__compose, cache_default=False)
    fc.source = f"stl.compose([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    fc.__name__ = f"compose_{''.join([func.__name__[:2] if not func.stl else func.__name__ for func in funcs])}"
    fc.stl = True
    return fc


# TODO: reimplement using sklearn.preprocessing.OHE
def make_ohe(cols, sep='_ohe_'):
    def __make_ohe(df):
        return pd.get_dummies(df.df[cols], prefix_sep=sep, columns=cols)

    return FeatureConstructor(__make_ohe, cache_default=False)


def target_encoding(cols, target_col, aggregation='mean', prefix='me_'):
    def __target_encoding(df):
        res = empty_like(df)
        if not cols:
            cols = get_categorical(df)
        for col in cols:
            if df.train:
                enc = df.groupby(col)[target_col].agg(aggregation)
                df.encoders['__me_' + col] = enc
            else:
                enc = df.encoders['__me_' + col]
            res[prefix + col] = df[col].map(enc)
        return res

    return FeatureConstructor(__target_encoding, cache_default=False)


def target_encode_list(cols, target_col, aggregation='mean', prefix='me_list_'):
    def __target_encode_list(df):
        res = empty_like(df)
        for col in cols:
            if df.train:
                enc = (pd.DataFrame(df[col].tolist(), index=df[target_col])
                       .stack()
                       .reset_index(name=col)
                       [[col, target_col]]).groupby(col).agg(aggregation)
                df.encoders['__me_list_' + col] = enc
            else:
                enc = df.encoders['__me_list_' + col]
            res[prefix + col] = df[col].apply(lambda x: [i[0] for i in enc.loc[x].values])
        return res

    return FeatureConstructor(__target_encode_list, cache_default=False)


import multiprocessing
import swifter


def apply(dataframe, function, **kwargs):
    """
    Applies function to dataframe faster.
    If n_threads is in kwargs and is greater than 1, applies by multiprocessing.
    :return: same as df.apply(function)
    """
    if 'n_threads' in kwargs:
        n_threads = kwargs.pop('n_threads')
    else:
        n_threads = 1
    if n_threads == 1:
        return dataframe.swifter.apply(function, **kwargs)

    def _apply_df(args):
        df, func, num, kw = args
        return num, df.swifter.apply(func, **kw)

    pool = multiprocessing.Pool(processes=n_threads)
    result = pool.map(_apply_df, [(d, function, i, kwargs) for i, d in enumerate(np.array_split(dataframe, n_threads))])
    pool.close()
    result = sorted(result, key=lambda x: x[0])
    return pd.concat([i[1] for i in result])


def get_categorical(df):
    if df.train:
        cat_features = [col for col in df.columns if df[col].dtype == object]
        df.encoders['__cat_features'] = cat_features
    else:
        cat_features = df.encoders['__cat_features']
    return cat_features


def get_numeric(df):
    if df.train:
        num_features = [col for col in df.columns if df[col].dtype != object]
        df.encoders['__num_features'] = num_features
    else:
        num_features = df.encoders['__num_features']
    return num_features