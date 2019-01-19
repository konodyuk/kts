from .. import config
import pandas as pd
import hashlib
import feather
import inspect
import dill
import os
from glob import glob

def get_src(func):
    src = inspect.getsource(func)
    if src[0] == '@':
        src = src[src.find('\n') + 1:]
    return src

def hash_df(df):
    return hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

def hash_src(func):
    src = get_src(func)
    return hashlib.sha256(src.encode()).hexdigest()


def save_df(df, path):
    index_name = f'{config.index_prefix}{df.index.name}'
    df[index_name] = df.index.values
    df.reset_index(drop=True, inplace=True)
    feather.write_dataframe(df, path)
    df.set_index(index_name, inplace=True)
    df.index.name = df.index.name[len(config.index_prefix):]
#     df.to_feather(path)
    
def load_df(path):
    tmp = feather.read_dataframe(path, use_threads=True)
    col = tmp.columns[tmp.columns.str.contains(config.index_prefix)].values[0]
    tmp.set_index(col, inplace=True)
    tmp.index.name = tmp.index.name[len(config.index_prefix):]
    return tmp

def save_src(func, path):
    with open(path, 'w') as f:
        f.write(get_src(func))
    
def load_src(path):
    with open(path, 'r') as f:
        src = f.read()
    return src

def save_fc(feature_constructor, path):
#     print('dumping: ', feature_constructor)
    dill.dump(feature_constructor, open(path, 'wb'))
    
def load_fc(path):
    return dill.load(open(path, 'rb'))


def make_df_name(func, df):
    return f"{func.__name__}__{hash_df(df)[:4]}.fth"
#     return f"{func.__name__}__{hash_src(func)[:4]}__{hash_df(df)[:4]}.fth"

def make_src_name(func):
    return f"{func.__name__}.py"
#     return f"{func.__name__}__{hash_src(func)[:4]}.py"

def make_fc_name(feature_constructor):
    return f"{feature_constructor.__name__}.fc"

    
def make_df_path(func, df):
    return config.feature_path + make_df_name(func, df)

def make_src_path(func):
    return config.feature_path + make_src_name(func)

def make_fc_path(feature_constructor):
    return config.feature_path + make_fc_name(feature_constructor)


def make_glob(name):
    return config.feature_path + name + '*'


def is_cached(func, df):
    return os.path.exists(make_df_path(func, df))

def cache(func, df_input, df_output):
    save_df(df_output, make_df_path(func, df_input))
    
def load_cached(func, df):
    return load_df(make_df_path(func, df))

def cache_src(func):
    save_src(func, make_src_path(func))
    
def load_src_func(func):
    def loader():
        return load_src(make_src_path(func))
    return loader

def cache_fc(feature_constructor):
    save_fc(feature_constructor, make_fc_path(feature_constructor))

def get_func_name_from_path(path):
    return path.split('/')[-1].split('.')[~1]

def get_func_name(src):
    return src.split(' ')[1].split('(')[0]
    
def is_cached_src(func):
    return os.path.exists(make_src_path(func))
    
def matches_cache(func):
    return get_src(func) == load_src(make_src_path(func))
    
def decache(force=False):
    def __decache(function):
        if not force:
            print("Are you sure you want to delete all the cached files?")
            print("To confirm please print full name of the function:")
            confirmation = input()
        if type(function) == str:
            if not force and confirmation != function:
                print("Doesn't match")
                return
            for path in glob(make_glob(function)):
                print(f'removing {path}')
                os.remove(path)
        else:
            if not force and confirmation != function.__name__:                
                print("Doesn't match")
                return
            for path in glob(make_glob(make_src_name(function))):            
                print(f'removing {path}')
                os.remove(path)
    return __decache
