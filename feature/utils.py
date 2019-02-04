from .. import config
import pandas as pd
import hashlib
import feather
import inspect
import dill
import os
from glob import glob


# SRC
def get_src(func):
    """
    Gets source code of a function. Ignores decorators starting with @.
    """
    src = inspect.getsource(func)
    if src[0] == '@':
        src = src[src.find('\n') + 1:]
    return src


def save_src(func, path):
    """
    Saves source code of a function as text.
    """
    with open(path, 'w') as f:
        f.write(get_src(func))


def load_src(path):
    """
    Loads source code of a function as text.
    """
    with open(path, 'r') as f:
        src = f.read()
    return src


def make_src_name(func):
    """
    Makes the name for a text file with the source code of a function.
    """
    return f"{func.__name__}.py"


def cache_src(func):
    """
    Saves the source code of a function func as a text file with proper path.
    """
    save_src(func, make_src_path(func))


def make_src_path(func):
    """
    Makes the path for a text file with the source code of a function.
    """
    return config.feature_path + make_src_name(func)


def is_cached_src(func):
    """
    Check if text file (actually .py file) fith function's source code exists.
    """
    return os.path.exists(make_src_path(func))


def load_src_func(func):
    def loader():
        return load_src(make_src_path(func))

    return loader


def get_func_name(src):
    """
    Gets name of a function from its source code.
    """
    return src.split(' ')[1].split('(')[0]


def matches_cache(func):
    """
    Checks if source code of a function is the same as that loaded from the proper path.
    """
    return get_src(func) == load_src(make_src_path(func))


# DF
def hash_df(df):
    """
    Hashes a dataframe.
    Each row of N rows is initially hashed (DEC), then all N hashed rows are hashed (HEX).
    Returns a single str.
    """
    return hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest()


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


def make_df_name(func, df):
    """
    Makes the name for a .fth binary file.
    """
    return f"{func.__name__}__{hash_df(df)[:4]}.fth"


def make_df_path(func, df):
    """
    Makes the path for a .fth binary file. This file caches a dataframe df transformed
    by function func so that we can use a transformed dataframe later.
    """
    return config.feature_path + make_df_name(func, df)


# FC
def save_fc(feature_constructor, path):
    """
    Save feature in "upgraded" pickle format
    """
    dill.dump(feature_constructor, open(path, 'wb'))


def load_fc(path):
    """
    Opposite to save_fc
    """
    return dill.load(open(path, 'rb'))


def make_fc_name(feature_constructor):
    """
    Makes the name for a .fc binary file.
    """
    return f"{feature_constructor.__name__}.fc"


def make_fc_path(feature_constructor):
    """
    Makes the path for a .fc binary file.
    """
    return config.feature_path + make_fc_name(feature_constructor)


# STUFF
def make_glob(name):
    """
    Constructs a pattern for glob. Glob searches all path matching this pattern. This finction is used in decache().
    """
    return glob(config.feature_path + name + '.*') + glob(
        config.feature_path + name + '__[0-9a-f][0-9a-f][0-9a-f][0-9a-f].*')


def is_cached(func, df):
    """
    Checks if a .fth file for certain df and func already exists.
    """
    return os.path.exists(make_df_path(func, df))


def cache(func, df_input, df_output):
    """
    Creates that binary .fth file for dataframe df_input transformed by function func into df_output
    with proper path.
    """
    save_df(df_output, make_df_path(func, df_input))


def load_cached(func, df):
    """
    Loads cached .fth file from proper path.
    """
    return load_df(make_df_path(func, df))


def cache_fc(feature_constructor):
    """
    Saves a function as .fc binary file with proper path.
    """
    save_fc(feature_constructor, make_fc_path(feature_constructor))


def get_func_name_from_path(path):
    """
    Gets name of a function from the path it is stored.
    """
    return path.split('/')[-1].split('.')[~1]


def decache(force=False):
    """
    Removes all paths for any cached files (.py, .fc, .fth) for particular function.
    """

    def __decache(function):
        if not force:
            print("Are you sure you want to delete all the cached files?")
            print("To confirm please print full name of the function:")
            confirmation = input()
        if type(function) == str:
            if not force and confirmation != function:
                print("Doesn't match")
                return
            for path in make_glob(function):
                print(f'removing {path}')
                os.remove(path)
        else:
            if not force and confirmation != function.__name__:
                print("Doesn't match")
                return
            for path in make_glob(function.__name__):
                print(f'removing {path}')
                os.remove(path)

    return __decache

