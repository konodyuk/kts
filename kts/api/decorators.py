from IPython.display import display

from kts.core.runtime import FeatureConstructor, GenericFeatureConstructor, run_manager
from kts.core.cache import frame_cache
from kts.core.frame import KTSFrame
from kts.core.lists import feature_list, helper_list


def preview(frame, *sizes):
    def _preview(obj):
        if isinstance(obj, GenericFeatureConstructor):
            feature_constructor = obj()
        else:
            feature_constructor = FeatureConstructor(obj)
        for size in sizes:
            ktsframe = KTSFrame(frame.head(size))
            ktsframe.__meta__['fold'] = 'preview'
            ktsframe.__meta__['run_manager'] = run_manager
            ktsframe.__meta__['frame_cache'] = frame_cache
            display(feature_constructor(ktsframe))
    return _preview


def feature(*args, cache=True, parallel=True):
    def register(function):
        if not isinstance(function, GenericFeatureConstructor):
            feature_constructor = FeatureConstructor(function)
        else:
            feature_constructor = function
        feature_constructor.cache = cache
        feature_constructor.parallel = parallel
        feature_list.register(feature_constructor)
        return feature_constructor
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

