from . import utils
from .. import config
from .storage import FeatureConstructor
from IPython.display import display
from glob import glob
import os


def test(function):
    
    def new_function(df, sizes=[2, 4, 6]):
        config.test_call = 1
        for sz in sizes:
            display(function(df.head(sz)))
        config.test_call = 0
    
    return new_function


def register(*args, cache_default=True):
    def __register(function):
        if utils.is_cached_src(function) and not utils.matches_cache(function):
            raise NameError("A function with the same name is already registered:\n")

        functor = FeatureConstructor(function, cache_default)
        # TODO: delete redundant recaching
        if not utils.is_cached_src(function):
            utils.cache_fc(functor)    
            utils.cache_src(function)
        return functor
    if args:
        function = args[0]
        return __register(function)
    else:
        return __register


def deregister(*args, force=False):
    if args:
        function = args[0]
        utils.decache(force)(function)
    else:
        return utils.decache(force)
    

def dropper(function):
    return register(function, cache_default=False)


def selector(function):
    return register(function, cache_default=False)
