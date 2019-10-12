from .storage import FeatureConstructor
import pandas as pd
import numpy as np
import copy
from ..storage.dataframe import DataFrame as KTDF
from ..zoo.cluster import KMeansFeaturizer
from sklearn.preprocessing import StandardScaler
from ..utils import list_hash, extract_signature, is_helper
# from joblib import Parallel, delayed


def wrap_stl_function(outer_function, inner_function):
    """

    Args:
      outer_function: 
      inner_function: 

    Returns:

    """
    source = f'stl.{outer_function.__name__}({extract_signature(outer_function)})'
    fc = FeatureConstructor(inner_function, cache_default=False, stl=True)
    fc.source = source
    fc.args = extract_signature(outer_function, return_dict=True)
    return fc


def __empty_like(df):
    if isinstance(df, KTDF):
        return KTDF(df[[]].copy(), df.train, df.encoders, df.slice_id)
    else:
        return KTDF(df[[]].copy())


empty_like = FeatureConstructor(__empty_like, cache_default=False)
empty_like.stl = True
empty_like.source = "stl.empty_like"


identity = FeatureConstructor(lambda df: df, cache_default=False)
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
    funcs = sum([i.args['funcs'] if i.source.startswith('stl.concat') else [i] for i in funcs], [])

    def __concat(df):
        return merge([func(df) for func in funcs])

    # fc = FeatureConstructor(__concat, cache_default=False)
    # fc.source = f"stl.concat([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    # fc.__name__ = f"concat_{''.join([func.__name__[:2] if not func.stl else func.__name__ for func in funcs])}"
    # fc.stl = True
    fc = wrap_stl_function(concat, __concat)
    fc.source = f"stl.concat([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    return fc


def compose(funcs):
    """

    Args:
      funcs: 

    Returns:

    """
    if len(funcs) == 2 and funcs[0].source.startswith('stl.concat') and funcs[1].source.startswith('stl.column_selector'):
        selection_list = funcs[1].args['columns']
        return concat([func + selection_list for func in funcs[0].args['funcs']])

    def __compose(df):
        res = identity(df)
        for func in funcs:
            res = func(res)
        return res

    # fc = FeatureConstructor(__compose, cache_default=False)
    # fc.source = f"stl.compose([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    # fc.__name__ = f"compose_{''.join([func.__name__[:2] if not func.stl else func.__name__ for func in funcs])}"
    # fc.stl = True
    fc = wrap_stl_function(compose, __compose)
    fc.source = f"stl.compose([{', '.join([func.__name__ if not func.stl else func.source for func in funcs])}])"
    return fc


# TODO: reimplement using sklearn.preprocessing.OHE
def make_ohe(cols, sep='_ohe_'):
    """

    Args:
      cols: 
      sep:  (Default value = '_ohe_')

    Returns:

    """
    def __make_ohe(df):
        return pd.get_dummies(df[cols], prefix_sep=sep, columns=cols)

    # return FeatureConstructor(__make_ohe, cache_default=False)
    return wrap_stl_function(make_ohe, __make_ohe)


# TODO: get helper-objects as args, not only functions, with fit() and transform() methods,
#       then call tfm.fit(df) and ..groupby().agg(tfm.transform)
#       will be useful for weighted target encoding,
#       like (df.sum() + C * global_mean) / (df.count() + C)
#       where global_mean is to be extracted inside of fit() call
# TODO: set global_answer var and .fillna(global_answer) for each pair of columns (for new classes)
def target_encoding(cols, target_cols, aggregation='mean', sep='_te_'):
    """Template for creating target encoding FeatureConstructors

    Args:
      cols: columns to encode
      target_cols: columns which values are aggregated
      aggregation: name of built-in pandas aggregation or @helper-function (Default value = 'mean')
      sep: string separator used for column naming (Default value = '_te_')

    Returns:
      FeatureConstructor: df -> features

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
                res[f"{col}{sep}{target_col}_{aggregation_name}"] = df[col].map(enc).astype(np.float)
                res[f"{col}{sep}{target_col}_{aggregation_name}"].fillna(global_answer, inplace=True)
        return res

    # return FeatureConstructor(__target_encoding, cache_default=False)
    return wrap_stl_function(target_encoding, __target_encoding)


def target_encode_list(cols, target_col, aggregation='mean', prefix='me_list_'):
    """

    Args:
      cols: 
      target_col: 
      aggregation:  (Default value = 'mean')
      prefix:  (Default value = 'me_list_')

    Returns:

    """
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


def discretize(cols, bins, prefix='disc_'):
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
                res[prefix + str(bins) + '_' + col], enc = pd.cut(df[col], bins, retbins=True)
                df.encoders[f'__disc_{bins}_{col}'] = enc
            else:
                enc = df.encoders[f'__disc_{bins}_{col}']
                res[prefix + str(bins) + '_' + col] = pd.cut(df[col], enc)
        return res

    # return FeatureConstructor(__discretize, cache_default=False)
    return wrap_stl_function(discretize, __discretize)


def discretize_quantile(cols, bins, prefix='disc_q_'):
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
                res[prefix + str(bins) + '_' + col], enc = pd.qcut(df[col], bins, retbins=True)
                df.encoders[f'__disc_q_{bins}_{col}'] = enc
            else:
                enc = df.encoders[f'__disc_q_{bins}_{col}']
                res[prefix + str(bins) + '_' + col] = pd.cut(df[col], enc)
        return res

    # return FeatureConstructor(__discretize_quantile, cache_default=False)
    return wrap_stl_function(discretize_quantile, __discretize_quantile)


def kmeans_encoding(cols, n_clusters, target_col=None, target_importance=5.0, prefix='km_', **kwargs):
    """

    Args:
      cols: 
      n_clusters: 
      target_col:  (Default value = None)
      target_importance:  (Default value = 5.0)
      prefix:  (Default value = 'km_')
      **kwargs: 

    Returns:

    """
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


def standardize(cols=None, prefix='std_'):
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
                df.encoders[f'__std_{col}'] = encoder
            else:
                encoder = df.encoders[f'__std_{col}']
                res[prefix + col] = encoder.transform(df[[col]].values)
        return res

    # return FeatureConstructor(__standardize, cache_default=False)
    return wrap_stl_function(standardize, __standardize)


from ..validation.leaderboard import leaderboard as lb
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
                res[bad_indices] = merge([pd.DataFrame(data=pred, columns=col_names)
                                          for col_names, pred in zip(oof_col_names, preds)])
        except:
            preds = [lb[id_exp].predict(df) for id_exp in ids]
            res = merge([pd.DataFrame(data=pred, columns=col_names)
                                      for col_names, pred in zip(oof_col_names, preds)])
            res.set_index(df.index, inplace=True)
        return res

    return wrap_stl_function(stack, __stack)


from ..storage.caching import load
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


# TODO: implement
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
