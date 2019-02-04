from .. import config
import pandas as pd
import hashlib
import feather
import inspect
import dill
import os
from glob import glob


def get_path_df(name):
    return config.storage_path + name + '_dataframe'


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
