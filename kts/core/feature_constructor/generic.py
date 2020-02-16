import inspect
import warnings
from copy import copy
from functools import wraps

import forge

from kts.core.feature_constructor.user_defined import FeatureConstructor


class GenericFeatureConstructor:
    def __init__(self, func, kwargs):
        warnings.warn('Generic feature support is still experimental, so please avoid corner cases.')
        self.func = func
        self.name = func.__name__
        self.source = inspect.getsource(func)
        self.parallel = kwargs.pop('parallel', True)
        self.cache = kwargs.pop('cache', True)
        self.kwargs = kwargs
        self.__call__ = forge.sign(**{arg: forge.arg(arg) for arg in self.kwargs})(self.__call__)

    def __call__(self, **kwargs):
        instance_kwargs = copy(self.kwargs)
        for k, v in kwargs.items():
            if k not in self.kwargs:
                raise ValueError(f"Unexpected arg: {k}")
            instance_kwargs[k] = v
        res = FeatureConstructor(self.modify(self.func, instance_kwargs))
        res.name = f"{self.name}_" + "_".join(map(str, instance_kwargs.values()))
        res.source = f"{self.name}({', '.join(f'{k}={repr(v)}' for k, v in instance_kwargs.items())})"
        res.parallel = self.parallel
        res.cache = self.cache
        return res

    def modify(self, func, instance_kwargs):
        @wraps(func)
        def new_func(*args, **kwargs):
            g = func.__globals__
            old_values = dict()
            for k, v in instance_kwargs.items():
                old_values[k] = g.get(k, None)
                g[k] = v

            try:
                res = func(*args, **kwargs)
            finally:
                for k, v in old_values.items():
                    if v is None:
                        del g[k]
                    else:
                        g[k] = v
            return res
        return new_func
