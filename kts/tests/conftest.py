import numpy as np
import pandas as pd
import pytest
from sklearn.model_selection import KFold

from kts.core.backend.ray_middleware import setup_ray
from kts.core.backend.run_manager import RunManager
from kts.core.cache import obj_cache, frame_cache
from kts.ui.feature_computing_report import FeatureComputingReport

setup_ray()

SIZE = 10000


@pytest.fixture
def clear_caches():
    for key in obj_cache.ls():
        del obj_cache[key]
    for key in frame_cache.ls():
        del frame_cache[key]


@pytest.fixture
def int_frame():
    return pd.DataFrame({'a': range(SIZE // 2)})


@pytest.fixture
def other_int_frame():
    return pd.DataFrame({'a': range(SIZE // 2, SIZE)})


@pytest.fixture
def frame():
    return pd.DataFrame({
        'int': range(SIZE),
        'str': [f"s_{i % 13}" for i in range(SIZE)],
        'float': np.linspace(0, 1, SIZE),
        'int_rand': np.random.randint(0, 10000, size=SIZE),
        'float_rand': np.random.rand(SIZE)
    })


@pytest.fixture
def folds():
    kf = KFold(5, False, 42)
    return list(kf.split(range(SIZE), range(SIZE)))


@pytest.fixture
def run_manager():
    res = RunManager()
    return res


@pytest.fixture
def report():
    return FeatureComputingReport(None)
