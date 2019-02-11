from .storage import FeatureConstructor
import pandas as pd
import numpy as np
from ..storage.dataframe import DataFrame as KTDF


def empty_like(df):
    return df[[]].copy()

identity = FeatureConstructor(lambda df: df, cache_default=False)


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


def make_mean_encoding(cols, target_col, prefix='me_'):
    def __make_mean_encoding(df):
        # print("ME call: ", type(df))
        # print(df.train)
        res = empty_like(df)
        for col in cols:
            res[prefix + col] = 0
            if df.train:
                enc = dict()
                for value in set(df[col]):
                    idxs = (df[col] == value)
                    enc[value] = res.loc[idxs, prefix + col] = df[idxs][target_col].mean()
                df.encoders['__me_' + col] = enc
            else:
                enc = df.encoders['__me_' + col]
                for value in set(df[col]):
                    if value in enc.keys():
                        encoding_value = enc[value]
                    else:
                        encoding_value = np.mean(list(enc.values()))
                        # print(f'Unknown value: {value}, inplacing with {encoding_value}')
                    idxs = (df[col] == value)
                    res.loc[idxs, prefix + col] = encoding_value
        return res

    return FeatureConstructor(__make_mean_encoding, cache_default=False)
