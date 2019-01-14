from . import utils
from IPython.display import display
from glob import glob
import os

def test(function):
    
    def new_function(df, sizes=[2, 4, 6]):
        for sz in sizes:
            display(function(df.head(sz)))
    
    return new_function

# not implemented "no_cache_default" parameter
# TODO: rewrite as a class-based decorator: 
# https://stackoverflow.com/questions/20093811/how-do-i-change-the-representation-of-a-python-function
# return something like <feature constructor "make_such_features">
# it is needed to implement a list of feature constructors, which can be used to share features between several notebooks
# e.g. 
# @property
# def feature_constructors(self):
#     load_smth()
# kts.feature_constructors -> [feature_1, feature_2] instead of [<function "0xfoo">, <function "0xbar">]
def register(function, no_cache_default=False):
    if utils.is_cached_src(function) and not utils.matches_cache(function):
        raise NameError(f"A function with the same name is already registered:\n{utils.load_src_func(function)()}")
    utils.cache_src(function)

    def new_function(df, no_cache=no_cache_default):
        if no_cache:
            return function(df)
        if utils.is_cached(function, df):
            return utils.load_cached(function, df)
        else:
            result = function(df)
            utils.cache(function, df, result)
            return result

    new_function.__name__ = function.__name__
    new_function.source = utils.load_src_func(function)
#     print(new_function.source())
    return new_function

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