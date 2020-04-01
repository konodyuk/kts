import inspect

import pandas as pd

from kts.core.backend.run_manager import run_cache
from kts.core.feature_constructor.parallel import ParallelFeatureConstructor
from kts.core.frame import KTSFrame
from kts.util.misc import extract_requirements, validate_source


class FeatureConstructor(ParallelFeatureConstructor):
    parallel = True
    cache = True
    verbose = True

    def __init__(self, func, internal=False):
        self.func = func
        if internal:
            return
        self.name = func.__name__
        self.description = func.__doc__
        self.source = inspect.getsource(func)
        validate_source(self.source)
        self.requirements = extract_requirements(func)
        self.dependencies = self.extract_dependencies(func)
        self.registered = True

    def compute(self, kf: KTSFrame):
        kwargs = {key: self.request_resource(value, kf) for key, value in self.dependencies.items()}
        result = self.func(kf, **kwargs)
        assert result.shape[0] == kf.shape[0]
        if isinstance(result, pd.DataFrame):
            assert all(result.index == kf.index)
        else:
            result = pd.DataFrame(data=result,
                                  index=kf.index,
                                  columns=[f"{self.name}_{i}" for i in range(result.shape[1])])
        if (not kf.train and '__columns' in kf._state
            and not (len(result.columns) == len(kf._state['__columns'])
                     and all(result.columns == kf._state['__columns']))):
            fixed_columns = kf._state['__columns']
            for col in set(fixed_columns) - set(result.columns):
                result[col] = None
            return result[fixed_columns]
        if '__columns' not in kf._state:
            kf._state['__columns'] = list(result.columns)
        return result

    def extract_dependencies(self, func):
        dependencies = dict()
        for k, v in inspect.signature(func).parameters.items():
            if isinstance(v.default, str):
                dependencies[k] = v.default
            elif v.default != inspect._empty:
                raise UserWarning(f"Unsupported argument type: {k}={type(v.default)}. String values expected.")
        return dependencies
