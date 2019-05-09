from ..storage import source_utils
from ..storage.caching import cache
from .. import config
from .storage import FeatureConstructor
from ..storage import dataframe
from IPython.display import display
from glob import glob
import os


def preview(df, sizes=(2, 4, 6)):
    """
    Applies function to heads of particular dataframe.
    
    Example:
    ``` python
    @preview(df, sizes=[5, 15])
    def make_ohe_pclass(df):
        ...
    ```
    """

    def __preview(function):
        config.preview_call = 1
        try:
            for sz in sizes:
                if isinstance(df, dataframe.DataFrame):
                    ktdf = dataframe.DataFrame(df.head(sz), df.train, df.encoders)
                else:
                    ktdf = dataframe.DataFrame(df.head(sz), True, {})
                # ktdf = dataframe.DataFrame(df.head(sz), True, {})
                display(function(ktdf))
        except Exception as e:
            config.preview_call = 0
            raise e
        config.preview_call = 0

    return __preview


def register(*args, cache_default=True):
    """
    Registers function for further caching its calls and restoring source.
    
    Example:
    ``` python
    @register
    def make_ohe_pclass(df):
        ...
    ```
    """

    def __register(func):
        # if source_utils.source_is_saved(func) and not source_utils.matches_cache(func):
        if func.__name__ + '_fc' in cache.cached_objs() and source_utils.get_source(func) != cache.load_obj(func.__name__ + '_fc').source:
            raise NameError("A function with the same name is already registered")

        if func.__name__ + '_fc' in cache.cached_objs():
            return cache.load_obj(func.__name__ + '_fc')
        else:
            functor = FeatureConstructor(func, cache_default)
            cache.cache_obj(functor, functor.__name__ + '_fc')
            return functor

    if args:
        function = args[0]
        return __register(function)
    else:
        return __register


def deregister(name, force=False):
    """
    Deletes sources and cached calls of a certain function.
    Usage:

    ``` python
    deregister('make_new_features')

    deregister('make_new_features', force=True)
    ```
    """
    confirmation = ''
    fc_name = name + '_fc'
    df_names = [df_name for df_name in cache.cached_dfs() if df_name.startswith(name + '__')]

    if not force:
        print("Are you sure you want to delete all these cached files?")
        if fc_name in cache.cached_objs():
            print(fc_name)
        for df_name in df_names:
            print(df_name)
        print("To confirm please print full name of the function:")
        confirmation = input()
    if not force and confirmation != name:
        print("Doesn't match")
        return

    if fc_name in cache.cached_objs():
        print(f'removing {fc_name}')
        cache.remove_obj(fc_name)
    for df_name in df_names:
        print(f'removing {df_name}')
        cache.remove_df(df_name)


def dropper(function):
    """
    Registers function that won't be cached.
    Is recommended to be used only with functions which actually drop columns or rows and don't produce any new data.
    
    Example:
    ``` python
    @dropper
    def drop_pclass(df):
        return stl.column_dropper(['Pclass'])(df)
    ```
    """
    #     TODO:
    #     if cache.is_cached(function.__name__):
    #         print('Dropper is already registered. Deregistering: ')
    #         deregister(function.__name__, force=True)
    deregister(function.__name__, force=True)
    return register(function, cache_default=False)


def selector(function):
    """
    Registers function that won't be cached.
    Is recommended to be used only with functions which actually select columns or rows and don't produce any new data.
    
    Example:
    ``` python
    @selector
    def select_pclass_cabin(df):
        return stl.column_selector(['Pclass', 'Cabin'])(df)
    ```
    """

    deregister(function.__name__, force=True)
    return register(function, cache_default=False)


def helper(func):
    """
    Save function as helper to store its source
    and be able to define it in any notebook with kts.helpers.define_in_scope()

    :param func: function
    :return: function with .source method
    """
    assert '__name__' in dir(func), 'Helper should have a name'
    func.source = source_utils.get_source(func)
    if func.__name__ + '_helper' in cache.cached_objs():
        cache.remove_obj(func.__name__ + '_helper')
    cache.cache_obj(func, func.__name__ + '_helper')
    return func
