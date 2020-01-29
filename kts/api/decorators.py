from IPython.display import display

from kts.core.runtime import FeatureConstructor, GenericFeatureConstructor
from kts.core.frame import KTSFrame
from kts.core.lists import feature_list, helper_list
import inspect

def create_feature_constructor(function):
    """Parses dependencies and returns FeatureConstructor object."""
    dependencies = dict()
    for k, v in inspect.signature(function):
        if isinstance(v.default, str):
            dependencies[k] = v.default 
        elif not isinstance(v.default, inspect._empty):
            raise UserWarning(f"Unknown argument: {k}={repr(v.default)}.")
    feature_constructor = FeatureConstructor(function, dependencies)
    return feature_constructor


def preview(frame, *sizes):
    def _preview(obj):
        if isinstance(obj, GenericFeatureConstructor):
            feature_constructor = obj()
        else:
            feature_constructor = create_feature_constructor(function)
        for size in sizes:
            ktsframe = KTSFrame(frame.head(size))
            ktsframe.__meta__['fold'] = 'preview'
            display(feature_constructor(ktsframe))
    return _preview


def feature(*args, cache=True, parallel=True):
    def register(function):
        if not isinstance(function, GenericFeatureConstructor):
            feature_constructor = create_feature_constructor(function)
        else:
            feature_constructor = function
        feature_constructor.cache = cache
        feature_constructor.parallel = parallel
        feature_list.register(feature_constructor)
    if args:
        function = args[0]
        return register(function)
    else:
        return register


def helper(function):
    helper_list.register(function)


def generic(**kwargs):
    def __generic(func):
        return GenericFeatureConstructor(func, kwargs)
    return __generic

# TODO
# def version():

