from IPython.display import display

from kts.core.backend.run_manager import run_manager
from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.feature_constructor.generic import GenericFeatureConstructor, create_generic
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.core.lists import feature_list, helper_list
from kts.ui.feature_computing_report import FeatureComputingReport


def preview(frame, *sizes, parallel=True, train=True):
    def _preview(obj):
        report = FeatureComputingReport(feature_list)
        if isinstance(obj, GenericFeatureConstructor):
            feature_constructor = obj()
        elif not isinstance(obj, BaseFeatureConstructor): # in case of stl preview
            feature_constructor = FeatureConstructor(obj)
        else:
            feature_constructor = obj
        feature_constructor.parallel = parallel
        try:
            for size in sizes:
                results = run_manager.run([feature_constructor],
                                          frame=frame.head(size),
                                          train=train,
                                          fold='preview',
                                          ret=True,
                                          report=report)
                display(results[feature_constructor.name])
        finally:
            run_manager.merge_scheduled()
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
        return create_generic(func, kwargs)
    return __generic

# TODO
# def version():

# TODO
# def parallel():
