from ..storage import source_utils
from ..storage.caching import cache
from .. import config
from .storage import FeatureConstructor
from IPython.display import display
from glob import glob
import os


def test(function):
    def new_function(df, sizes=(2, 4, 6)):
        config.test_call = 1
        for sz in sizes:
            display(function(df.head(sz)))
        config.test_call = 0

    return new_function


def register(*args, cache_default=True):
    """

    :param args:
    :param cache_default:
    :return:
    """
    def __register(func):
        if source_utils.source_is_saved(func) and not source_utils.matches_cache(func):
            raise NameError("A function with the same name is already registered")

        functor = FeatureConstructor(func, cache_default)
        if not source_utils.source_is_saved(func):
            cache.cache_obj(functor)
            source_utils.save_source(func)
        return functor

    if args:
        function = args[0]
        return __register(function)
    else:
        return __register


def deregister(name, force=False):
    """
    Removes everything related with function from the cache.
    :param name: function name
    :param force: True or False (requires confirmation or not)
    :return:
    """
    confirmation = ''
    if not force:
        print("Are you sure you want to delete all the cached files?")
        print("To confirm please print full name of the function:")
        confirmation = input()
    if not force and confirmation != name:
        print("Doesn't match")
        return

    paths = glob(config.feature_path + name + '.*') + glob(
        config.feature_path + name + '__[0-9a-f][0-9a-f][0-9a-f][0-9a-f].*')
    for path in paths:
        print(f'removing {path}')
        os.remove(path)


def dropper(function):
    return register(function, cache_default=False)


def selector(function):
    return register(function, cache_default=False)
