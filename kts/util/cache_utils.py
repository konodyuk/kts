import hashlib
import os
from glob import glob

import dill
import feather
import numpy as np
import pandas as pd

from kts.util.misc import captcha
from kts import config


def clear_storage():
    """ """
    from kts.core.backend.memory import cache

    cache.memory.clear()
    if not captcha():
        print("You aren't smart enough to take such decisions")
        return
    for path in glob(config.STORAGE_PATH + "*_*"):
        print(f"deleting {path}")
        os.remove(path)


def get_hash_df(df):
    """

    Args:
      df: 

    Returns:

    """
    idx_hash = hashlib.sha256(pd.util.hash_pandas_object(
        df.index).values).hexdigest()

    sorted_cols = df.columns.sort_values()
    col_hash = hashlib.sha256(
        pd.util.hash_pandas_object(sorted_cols).values).hexdigest()
    try:
        hash_first, hash_last = pd.util.hash_pandas_object(
            df.iloc[[0, -1]][sorted_cols]).values
    except:
        hash_first = hashlib.sha256(
            df.iloc[[0]][sorted_cols].to_csv().encode("utf-8")).hexdigest()
        hash_last = hashlib.sha256(
            df.iloc[[-1]][sorted_cols].to_csv().encode("utf-8")).hexdigest()

    return hashlib.sha256(np.array([idx_hash, col_hash, hash_first,
                                    hash_last])).hexdigest()

    # return hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest()


def get_hash_slice(idxs):
    """

    Args:
      idxs: 

    Returns:

    """
    if isinstance(idxs, slice):
        idxs = (-1337, idxs.start, idxs.stop, idxs.step)
    return hex(hash(frozenset(idxs)))[2:]


def get_df_volume(df):
    """

    Args:
      df: 

    Returns:

    """
    return df.memory_usage(index=True).sum()


def get_path_df(name):
    """

    Args:
      name: 

    Returns:

    """
    return config.STORAGE_PATH + name + "_df"


def save_df(df, path):
    """Saves a dataframe as feather binary file. Adds to df and additional column filled
    with index values and having a special name.

    Args:
      df: 
      path: 

    Returns:

    """
    # print('saving df', path)
    # try:
    #     print('enc', df.encoders)
    # except:
    #     print('enc: no attr')
    not_trivial_index = type(df.index) != pd.RangeIndex
    if not_trivial_index:
        index_name = f"{config.INDEX_COLUMN_PREFIX}{df.index.name}"
        df[index_name] = df.index.values
        df.reset_index(drop=True, inplace=True)
    try:
        feather.write_dataframe(df, path)
    except Exception as e:
        print(e)
        try:
            df.to_parquet(path)
        except Exception as e1:
            print(e1)
            df.to_pickle(path)
    if not_trivial_index:
        df.set_index(index_name, inplace=True)
        df.index.name = df.index.name[len(config.INDEX_COLUMN_PREFIX):]


def load_df(path):
    """Loads a dataframe from feather format and sets as index that additional
    column added with saving by save_df. Restores original name of index column.

    Args:
      path: 

    Returns:

    """
    try:
        tmp = feather.read_dataframe(path, use_threads=True)
    except:
        try:
            tmp = pd.read_parquet(path)
        except:
            tmp = pd.read_pickle(path)
    index_col = tmp.columns[tmp.columns.str.contains(config.INDEX_COLUMN_PREFIX)]
    if any(index_col):
        tmp.set_index(index_col.values[0], inplace=True)
        tmp.index.name = tmp.index.name[len(config.INDEX_COLUMN_PREFIX):]
    return tmp


def get_path_obj(name):
    """

    Args:
      name: 

    Returns:

    """
    return config.STORAGE_PATH + name + "_obj"


def save_obj(obj, path):
    """Saves object

    Args:
      obj: 
      path: 

    Returns:

    """
    try:
        dill.dump(obj, open(path, "wb"))
    except dill.PicklingError:
        if os.path.exists(path):
            os.remove(path)
            raise Warning(f"PicklingError occured, removing {path}")


def load_obj(path):
    """Loads object

    Args:
      path: 

    Returns:

    """
    return dill.load(open(path, "rb"))


def get_time(path):
    """

    Args:
      path: 

    Returns:

    """
    return int(os.path.getmtime(path))
