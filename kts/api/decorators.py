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
from kts.ui.docstring import html_docstring
from kts.ui.feature_computing_report import FeatureComputingReport
from kts.ui.settings import update_dashboard


@html_docstring
def preview(frame, *sizes, parallel=True, train=True):
    """Runs a feature constructor several times to let you make sure it works correctly

    Sequentially passes frame.head(size) to your feature constructor
    for each provided size.
    Generic features can also be previewed, in this case they'll be
    initialized using their default arguments.

    Args:
        frame: a dataframe to be used for testing your feature
        *sizes: one or more ints, sizes of input dataframes
        parallel: whether to preview as a parallel feature constructor
        train: df.train flag value to be passed to the feature constructor

    Examples:
        >>> @preview(train, 2, 3, parallel=False)
        ... def some_feature(df):
        ...     res = stl.empty_like(df)
        ...     res['col'] = ...
        ...     return res

        >>> @preview(train, 200)
        ... def some_feature(df):
        ...     return stl.mean_encode(['Parch', 'Embarked'], 'Survived')(df)

        >>> @preview(train, 100)
        ... @generic(left="Age", right="SibSp")
        ... def numeric_interactions(df):
        ...     res = stl.empty_like(df)
        ...     res[f"{left}_add_{right}"] = df[left] + df[right]
        ...     res[f"{left}_sub_{right}"] = df[left] - df[right]
        ...     res[f"{left}_mul_{right}"] = df[left] * df[right]
        ...     return res
    """
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
            cfg.feature_computing_report = report
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
            cfg.feature_computing_report = None
    return _preview


@html_docstring
def feature(*args, cache=True, parallel=True, verbose=True):
    """Registers a function as a feature constructor and saves it

    Can be used both with and without flags.
    Note that generic feature constructors should be
    additionally registered using this decorator.

    Args:
        cache: whether to cache calls and avoid recomputing
        parallel: whether to run in parallel with other parallel FCs
        verbose: whether to print logs and show progress

    Returns:
        A feature constructor.

    Examples:
        >>> @feature(parallel=False, verbose=False)
        ... def some_feature(df):
        ...     ...

        >>> @feature
        ... def some_feature(df):
        ...     ...

        >>> @feature
        ... @generic(param='default')
        ... def generic_feature(df):
        ...     ...
    """
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


@html_docstring
def helper(function):
    """Registers a helper

    A helper is an uncached function with any signature.
    Usually they're used to define custom metrics or something else.

    Args:
        function: any function

    Returns:
        A helper instance.

    Examples:
        >>> @helper
        ... def sophisticated_metric(y_true, y_pred, groups):
        ...     ...
    """
    helper = Helper(function)
    helper_list.register(helper)
    update_dashboard()
    return helper


@html_docstring
def generic(**kwargs):
    """Creates a generic feature constructor

    Generic features are parametrized feature constructors.

    Note that this decorator does not register your function
    and you should add @feature to save it.

    Args:
        **kwargs: arguments and their default values

    Returns:
        A generic feature constructor.

    Examples:
        >>> @feature
        ... @generic(left="Age", right="SibSp")
        ... def numeric_interactions(df):
        ...     res = stl.empty_like(df)
        ...     res[f"{left}_add_{right}"] = df[left] + df[right]
        ...     res[f"{left}_sub_{right}"] = df[left] - df[right]
        ...     res[f"{left}_mul_{right}"] = df[left] * df[right]
        ...     return res

        >>> from itertools import combinations
        >>> fs = FeatureSet([
        ...     numeric_interactions(left, right)
        ...     for left, right in combinations(['Parch', 'SibSp', 'Age'], r=2)
        ... ], ...)
    """
    def __generic(func):
        return create_generic(func, kwargs)
    return __generic

# TODO
# def version():

# TODO
# def parallel():


@html_docstring
def delete(feature_or_helper: Union[ParallelFeatureConstructor, GenericFeatureConstructor, Helper], force=False):
    """Deletes given feature or helper from lists and clears cache

    Feature constructors are deleted along with their cache.
    Generic feature constructors are also fully deleted.
    As some STL features produce cache, you can also remove it
    by passing an STL feature as an argument. The STL feature itself won't be removed.

    Args:
        feature_or_helper: an instance to be removed
        force: force deletion without any warnings and confirmations

    Examples:
        >>> delete(incorrect_feature)
        >>> delete(old_helper)
        >>> delete(stl.mean_encode('Embarked', 'Survived'))
        >>> delete(generic_feature)
    """
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
