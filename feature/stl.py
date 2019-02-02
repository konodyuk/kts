from .storage import FeatureConstructor
from . import utils
from .utils import empty_like

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
        return utils.merge([func(df) for func in funcs])
    return FeatureConstructor(__compose, cache_default=False)

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

