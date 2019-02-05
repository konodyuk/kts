from .storage import FeatureConstructor
import pandas as pd


def empty_like(df):
    return df[[]].copy()

identity = FeatureConstructor(lambda df: df, cache_default=False)

def merge(dfs):
    return pd.concat(dfs, axis=1)


def column_selector(columns):
    def __col_selector(df):
        return df[columns]

    return FeatureConstructor(__col_selector, cache_default=False)


def column_dropper(columns):
    def __col_dropper(df):
        return df.drop(columns, axis=1)

    return FeatureConstructor(__col_dropper, cache_default=False)


def compose(funcs):
    def __compose(df):
        return merge([func(df) for func in funcs])

    fc = FeatureConstructor(__compose, cache_default=False)
    fc.src = f"stl.compose([{', '.join([func.__name__ if not func.stl else func.src for func in funcs])}])"
    fc.__name__ = f"compose_{''.join([func.__name__[:2] if not func.stl else func.__name__ for func in funcs])}"
    fc.stl = True
    return fc


def sequence(funcs):
    def __sequence(df):
        res = empty_like(df)
        for func in funcs:
            res = merge([res, func(res)])
        return res

    fc = FeatureConstructor(__sequence, cache_default=False)
    fc.src = f"stl.sequence([{', '.join([func.__name__ if not func.stl else func.src for func in funcs])}])"
    fc.__name__ = f"sequence_{''.join([func.__name__[:2] if not func.stl else func.__name__ for func in funcs])}"
    fc.stl = True
    return fc


def make_ohe(cols, sep='_ohe_'):
    def __make_ohe(df):
        return pd.get_dummies(df[cols], prefix_sep=sep, columns=cols)

    return FeatureConstructor(__make_ohe, cache_default=False)


def make_mean_encoding(cols, target_col, prefix='me_'):
    def __make_mean_encoding(df):
        res = empty_like(df)
        for col in cols:
            res[prefix + col] = 0
            for value in set(df[col]):
                idxs = (df[col] == value)
                res.loc[idxs, prefix + col] = df[idxs][target_col].mean()
        return res

    return FeatureConstructor(__make_mean_encoding, cache_default=False)
