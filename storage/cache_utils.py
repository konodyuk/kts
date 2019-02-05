from .. import config
import hashlib
import pandas as pd
import feather
import dill
from glob import glob
import os
import numpy as np


def clear_storage():
    a, b = np.random.randint(10, size=2)
    c = int(input(f"{a} + {b} ="))
    if a + b != c:
        print("You aren't smart enough to take such decisions")
        return
    for path in glob(config.storage_path + '*_*') + glob(config.source_path + '*'):
        print(f"deleting {path}")
        os.remove(path)


def get_hash(df):
    return hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest()


def get_df_volume(df):
    return df.memory_usage(index=True).sum()


def get_path_df(name):
    return config.storage_path + name + '_df'


def save_df(df, path):
    """
    Saves a dataframe as feather binary file. Adds to df and additional column filled
    with index values and having a special name.
    """
    index_name = f'{config.index_prefix}{df.index.name}'
    df[index_name] = df.index.values
    df.reset_index(drop=True, inplace=True)
    feather.write_dataframe(df, path)
    df.set_index(index_name, inplace=True)
    df.index.name = df.index.name[len(config.index_prefix):]


def load_df(path):
    """
    Loads a dataframe from feather format and sets as index that additional
    column added with saving by save_df. Restores original name of index column.
    """
    tmp = feather.read_dataframe(path, use_threads=True)
    col = tmp.columns[tmp.columns.str.contains(config.index_prefix)].values[0]
    tmp.set_index(col, inplace=True)
    tmp.index.name = tmp.index.name[len(config.index_prefix):]
    return tmp


def get_path_obj(name):
    return config.storage_path + name + '_obj'


def save_obj(obj, path):
    """
    Saves object
    """
    dill.dump(obj, open(path, 'wb'))


def load_obj(path):
    """
    Loads object
    """
    return dill.load(open(path, 'rb'))


def get_path_info(name):
    return config.info_path + name + '_info'
