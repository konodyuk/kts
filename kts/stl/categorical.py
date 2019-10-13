import numpy as np
import pandas as pd

from kts.core.base_constructors import wrap_stl_function, empty_like
from kts.util.misc import is_helper
from kts.zoo.cluster import KMeansFeaturizer


def make_ohe(cols, sep="_ohe_"):
    """

    Args:
      cols:
      sep:  (Default value = '_ohe_')

    Returns:

    """
    def __make_ohe(df):
        return pd.get_dummies(df[cols], prefix_sep=sep, columns=cols)

    return wrap_stl_function(make_ohe, __make_ohe)


def target_encoding(cols, target_cols, aggregation="mean", sep="_te_"):
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
        raise TypeError(
            "aggregation parameter should be either string or helper")

    def __target_encoding(df):
        res = empty_like(df)
        for target_col in target_cols:
            for col in cols:
                if df.train:
                    enc = df.groupby(col)[target_col].agg(aggregation)
                    global_answer = df[target_col].agg(aggregation)
                    df.encoders[
                        f"__te_{col}_{target_col}_{aggregation_name}"] = enc
                    df.encoders[
                        f"__te_{col}_{target_col}_{aggregation_name}_global_answer"] = global_answer
                else:
                    enc = df.encoders[
                        f"__te_{col}_{target_col}_{aggregation_name}"]
                    global_answer = df.encoders[
                        f"__te_{col}_{target_col}_{aggregation_name}_global_answer"]
                res[f"{col}{sep}{target_col}_{aggregation_name}"] = (
                    df[col].map(enc).astype(np.float))
                res[f"{col}{sep}{target_col}_{aggregation_name}"].fillna(
                    global_answer, inplace=True)
        return res

    return wrap_stl_function(target_encoding, __target_encoding)


def target_encode_list(cols, target_col, aggregation="mean", prefix="me_list_"):
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
                enc = ((pd.DataFrame(
                    [i if type(i) == list else [i] for i in df[col].tolist()],
                    index=df[target_col],
                ).stack().reset_index(name=col)[[
                    col, target_col
                ]]).groupby(col).agg(aggregation))
                df.encoders["__me_list_" + col] = enc
            else:
                enc = df.encoders["__me_list_" + col]
            res[prefix + col] = df[col].apply(lambda x: enc.loc[filter(
                lambda i: i in enc.index, x)].values.flatten()
                                              if type(x) == list else filler)
        return res

    return wrap_stl_function(target_encode_list, __target_encode_list)


def kmeans_encoding(cols,
                    n_clusters,
                    target_col=None,
                    target_importance=5.0,
                    prefix="km_",
                    **kwargs):
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
        res_column_name = (
            f"{prefix}{n_clusters}_{list_hash(cols, 5)}_{round(target_importance, 4)}"
        )
        if df.train:
            encoder = KMeansFeaturizer(k=n_clusters,
                                       target_scale=target_importance,
                                       **kwargs)
            if target_col:
                res[res_column_name] = encoder.fit_transform(
                    df[cols].values, df[target_col].values)
            else:
                res[res_column_name] = encoder.fit_transform(df[cols].values)
            df.encoders[
                f"__kmeans_feat_{n_clusters}_{list_hash(cols, 5)}_{round(target_importance, 4)}"] = encoder
        else:
            encoder = df.encoders[
                f"__kmeans_feat_{n_clusters}_{list_hash(cols, 5)}_{round(target_importance, 4)}"]
            res[res_column_name] = encoder.transform(df[cols].values)
        return res

    return wrap_stl_function(kmeans_encoding, __kmeans_encoding)