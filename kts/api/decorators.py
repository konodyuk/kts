from typing import Union
from warnings import warn

from IPython.display import display

from kts.api.helper import Helper
from kts.core.backend.run_manager import run_manager, run_cache
from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.feature_constructor.generic import GenericFeatureConstructor, create_generic
from kts.core.feature_constructor.parallel import ParallelFeatureConstructor
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.core.lists import feature_list, helper_list
from kts.settings import cfg
from kts.ui.feature_computing_report import FeatureComputingReport
from kts.ui.settings import update_dashboard


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
                report.finish()
                display(results[feature_constructor.name])
        finally:
            run_manager.merge_scheduled()
    return _preview


def feature(*args, cache=True, parallel=True, verbose=True):
    def register(function):
        if not isinstance(function, GenericFeatureConstructor):
            feature_constructor = FeatureConstructor(function)
        else:
            feature_constructor = function
        feature_constructor.cache = cache
        feature_constructor.parallel = parallel
        feature_constructor.verbose = verbose
        feature_list.register(feature_constructor)
        update_dashboard()
        return feature_constructor
    if args:
        function = args[0]
        return register(function)
    else:
        return register


def helper(function):
    helper_list.register(Helper(function))
    update_dashboard()


def generic(**kwargs):
    def __generic(func):
        return create_generic(func, kwargs)
    return __generic

# TODO
# def version():

# TODO
# def parallel():


def delete(feature_or_helper: Union[ParallelFeatureConstructor, GenericFeatureConstructor, Helper], force=False):
    """TODO: add dependency checks"""
    if not force:
        warn(f"kts.delete() does not check if a feature or a helper is used in any experiments. "
             f"Please make sure it is not used anywhere.")
        confirmation = input("Are you sure? y/n")
        if confirmation[0] != 'y':
            return

    instance = feature_or_helper
    if isinstance(instance, ParallelFeatureConstructor):
        if instance.name in feature_list:
            del feature_list[instance.name]
        for args in instance.split(None):
            run_cache.del_feature(instance.get_scope(*args))
    elif isinstance(instance, GenericFeatureConstructor):
        if instance.name in feature_list:
            del feature_list[instance.name]
        run_cache.del_feature(instance.name)
    elif isinstance(instance, Helper):
        if instance.name in helper_list:
            del helper_list[instance.name]
    else:
        raise TypeError(f"Expected cached feature constructor, generic or helper, got {instance.__class__.__name__}.")

    update_dashboard()
    if instance.name not in cfg.scope:
        return
    if cfg.scope[instance.name] is instance:
        cfg.scope.pop(instance.name)
