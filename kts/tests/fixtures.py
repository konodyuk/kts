import pandas as pd
import pytest

from kts.core.backend.run_manager import RunManager
from kts.core.cache import obj_cache, frame_cache
from kts.core.frame import KTSFrame
from kts.ui.feature_computing_report import FeatureComputingReport


@pytest.fixture
def clear_caches():
    for key in obj_cache.ls():
        del obj_cache[key]
    for key in frame_cache.ls():
        del frame_cache[key]


@pytest.fixture
def ktsframe():
    res = KTSFrame(pd.DataFrame({'a': range(10000)}))
    return res


@pytest.fixture
def ktsframe_1():
    res = KTSFrame(pd.DataFrame({'a': range(5000)}))
    return res


@pytest.fixture
def ktsframe_2():
    res = KTSFrame(pd.DataFrame({'a': range(5000, 10000)}))
    return res


@pytest.fixture
def run_manager():
    res = RunManager()
    return res


@pytest.fixture
def report():
    return FeatureComputingReport(None)