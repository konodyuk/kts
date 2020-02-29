import pandas as pd
import pytest

from kts.core.backend.run_manager import RunManager
from kts.core.cache import obj_cache, frame_cache
from kts.ui.feature_computing_report import FeatureComputingReport


@pytest.fixture
def clear_caches():
    for key in obj_cache.ls():
        del obj_cache[key]
    for key in frame_cache.ls():
        del frame_cache[key]


@pytest.fixture
def int_frame():
    return pd.DataFrame({'a': range(5000)})


@pytest.fixture
def other_int_frame():
    return pd.DataFrame({'a': range(5000, 10000)})


@pytest.fixture
def run_manager():
    res = RunManager()
    return res


@pytest.fixture
def report():
    return FeatureComputingReport(None)