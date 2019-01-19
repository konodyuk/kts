from . import utils
from .storage import FeatureConstructor
from IPython.display import display
from glob import glob
import os

def test(function):
    
    def new_function(df, sizes=[2, 4, 6]):
        for sz in sizes:
            display(function(df.head(sz)))
    
    return new_function


def register(function, no_cache_default=False):
    if utils.is_cached_src(function) and not utils.matches_cache(function):
        raise NameError(f"A function with the same name is already registered:\n{utils.load_src_func(function)()}")

    functor = FeatureConstructor(function, no_cache_default)
    # TODO: delete redundant recaching
    if not utils.is_cached_src(function):
        utils.cache_fc(functor)    
        utils.cache_src(function)
    return functor

def deregister(*args, **kwargs):
#     print(args, kwargs)
    force = False
    if 'force' in kwargs:
        force = kwargs['force']
    if len(args) >= 1:
        function = args[0]
        utils.decache(force)(function)
    else:
        return utils.decache(force)