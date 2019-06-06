from .storage import FeatureConstructor
import pandas as pd
import numpy as np
from ..storage.dataframe import DataFrame as KTDF
from ..zoo.cluster import KMeansFeaturizer
from sklearn.preprocessing import StandardScaler
from ..utils import list_hash, extract_signature, is_helper
# from joblib import Parallel, delayed


def wrap_stl_function(outer_function, inner_function):
    source = f'stl.{outer_function.__name__}({extract_signature(outer_function)})'
    fc = FeatureConstructor(inner_function, cache_default=False, stl=True)
    fc.source = source
    return fc


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
        return df[[col for col in columns if col in df.columns]]

    # return FeatureConstructor(__col_selector, cache_default=False)
    return wrap_stl_function(column_selector, __col_selector)


def column_dropper(columns):
    def __col_dropper(df):
        return df.drop([col for col in columns if col in df.columns], axis=1)

    # return FeatureConstructor(__col_dropper, cache_default=False)
    return wrap_stl_function(column_dropper, __col_dropper)


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
        res = identity(df)
        for func in funcs:
            res = func(res)
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

    # return FeatureConstructor(__make_ohe, cache_default=False)
    return wrap_stl_function(make_ohe, __make_ohe)


# TODO: get helper-objects as args, not only functions, with fit() and transform() methods,
#       then call tfm.fit(df) and ..groupby().agg(tfm.transform)
#       will be useful for weighted target encoding,
#       like (df.sum() + C * global_mean) / (df.count() + C)
#       where global_mean is to be extracted inside of fit() call
# TODO: set global_answer var and .fillna(global_answer) for each pair of columns (for new classes)
def target_encoding(cols, target_cols, aggregation='mean', sep='_te_'):
    """ Template for creating target encoding FeatureConstructors

    :param cols: columns to encode
    :param target_cols: columns which values are aggregated
    :param aggregation: name of built-in pandas aggregation or @helper-function
    :param sep: string separator used for column naming
    :return: FeatureConstructor: df -> features
    """
    if type(cols) != list:
        cols = [cols]
    if type(target_cols) != list:
        target_cols = [target_cols]
    if isinstance(aggregation, str):
        aggregation_name = aggregation
    elif is_helper(aggregation):
        aggregation_name = aggregation.__name__
    else:
        raise TypeError('aggregation parameter should be either string or helper')

    def __target_encoding(df):
        res = empty_like(df)
        for target_col in target_cols:
            for col in cols:
                if df.train:
                    enc = df.groupby(col)[target_col].agg(aggregation)
                    global_answer = df[target_col].agg(aggregation)
                    df.encoders[f"__te_{col}_{target_col}_{aggregation_name}"] = enc
                    df.encoders[f"__te_{col}_{target_col}_{aggregation_name}_global_answer"] = global_answer
                else:
                    enc = df.encoders[f"__te_{col}_{target_col}_{aggregation_name}"]
                    global_answer = df.encoders[f"__te_{col}_{target_col}_{aggregation_name}_global_answer"]
                res[f"{col}{sep}{target_col}_{aggregation_name}"] = df[col].map(enc)
                res[f"{col}{sep}{target_col}_{aggregation_name}"].fillna(global_answer, inplace=True)
        return res

    # return FeatureConstructor(__target_encoding, cache_default=False)
    return wrap_stl_function(target_encoding, __target_encoding)


def target_encode_list(cols, target_col, aggregation='mean', prefix='me_list_'):
    def __target_encode_list(df):
        res = empty_like(df)
        filler = [np.nan]
        for col in cols:
            if df.train:
                enc = (pd.DataFrame([i if type(i) == list else [i] for i in df[col].tolist()], index=df[target_col])
                       .stack()
                       .reset_index(name=col)
                       [[col, target_col]]).groupby(col).agg(aggregation)
                df.encoders['__me_list_' + col] = enc
            else:
                enc = df.encoders['__me_list_' + col]
            res[prefix + col] = df[col].apply(lambda x: enc.loc[filter(lambda i: i in enc.index, x)].values.flatten() if type(x) == list else filler)
        return res

    # return FeatureConstructor(__target_encode_list, cache_default=False)
    return wrap_stl_function(target_encode_list, __target_encode_list)


import multiprocessing
import swifter


def _apply_df(args):
    df, func, num, kw = args
    return num, df.swifter.apply(func, **kw)


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


def discretize(cols, bins, prefix='disc_'):
    def __discretize(df):
        res = empty_like(df)
        for col in cols:
            if df.train:
                res[prefix + str(bins) + '_' + col], enc = pd.cut(df[col], bins, retbins=True)
                df.encoders[f'__disc_{bins}_{col}'] = enc
            else:
                enc = df.encoders[f'__disc_{bins}_{col}']
                res[prefix + str(bins) + '_' + col] = pd.cut(df[col], enc)
        return res

    # return FeatureConstructor(__discretize, cache_default=False)
    return wrap_stl_function(discretize, __discretize)


def discretize_quantile(cols, bins, prefix='disc_q_'):
    def __discretize_quantile(df):
        res = empty_like(df)
        for col in cols:
            if df.train:
                res[prefix + str(bins) + '_' + col], enc = pd.qcut(df[col], bins, retbins=True)
                df.encoders[f'__disc_q_{bins}_{col}'] = enc
            else:
                enc = df.encoders[f'__disc_q_{bins}_{col}']
                res[prefix + str(bins) + '_' + col] = pd.cut(df[col], enc)
        return res

    # return FeatureConstructor(__discretize_quantile, cache_default=False)
    return wrap_stl_function(discretize_quantile, __discretize_quantile)


def kmeans_encoding(cols, n_clusters, target_col=None, target_importance=5.0, prefix='km_', **kwargs):
    def __kmeans_encoding(df):
        res = empty_like(df)
        res_column_name = f'{prefix}{n_clusters}_{list_hash(cols, 5)}_{round(target_importance, 4)}'
        if df.train:
            encoder = KMeansFeaturizer(k=n_clusters, target_scale=target_importance, **kwargs)
            if target_col:
                res[res_column_name] = encoder.fit_transform(df[cols].values, df[target_col].values)
            else:
                res[res_column_name] = encoder.fit_transform(df[cols].values)
            df.encoders[f'__kmeans_feat_{n_clusters}_{list_hash(cols, 5)}_{round(target_importance, 4)}'] = encoder
        else:
            encoder = df.encoders[f'__kmeans_feat_{n_clusters}_{list_hash(cols, 5)}_{round(target_importance, 4)}']
            res[res_column_name] = encoder.transform(df[cols].values)
        return res

    # return FeatureConstructor(__kmeans_encoding, cache_default=False)
    return wrap_stl_function(kmeans_encoding, __kmeans_encoding)


def standardize(cols, prefix='std_'):
    def __standardize(df):
        res = empty_like(df)
        for col in cols:
            if df.train:
                encoder = StandardScaler()
                res[prefix + col] = encoder.fit_transform(df[[col]].values)
                df.encoders[f'__std_{col}'] = encoder
            else:
                encoder = df.encoders[f'__std_{col}']
                res[prefix + col] = encoder.transform(df[[col]].values)
        return res

    # return FeatureConstructor(__standardize, cache_default=False)
    return wrap_stl_function(standardize, __standardize)


from ..validation.leaderboard import leaderboard as lb
def stack(ids):
    def __stack(df):
        oof_preds = merge([lb[id_exp].oof for id_exp in ids])
        try:
            res = oof_preds[df.index]
            if res.isna().sum().sum() > 0:
                bad_indices = res.isna().sum(axis=1) > 0
                # try:
                #     preds = Parallel(n_jobs=-1)([delayed(lb[id_exp].predict)(df[bad_indices]) for id_exp in ids])
                # except Exception as e:
                #     raise e
                preds = [lb[id_exp].predict(df[bad_indices]) for id_exp in ids]
                res[bad_indices] = merge([pd.DataFrame({id_exp: pred}) for id_exp, pred in zip(ids, preds)])
        except:
            # try:
            #     preds = Parallel(n_jobs=-1)([delayed(lb[id_exp].predict)(df) for id_exp in ids])
            # except Exception as e:
            #     raise e
            preds = [lb[id_exp].predict(df) for id_exp in ids]
            res = merge([pd.DataFrame({id_exp: pred}) for id_exp, pred in zip(ids, preds)])
            res.set_index(df.index, inplace=True)
        return res

    return wrap_stl_function(stack, __stack)


# TODO: implement
def cv_apply(feature_constructor, split):
    raise NotImplementedError
    def __cv_apply(df):
        res = empty_like(df)
