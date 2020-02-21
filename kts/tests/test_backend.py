from itertools import product

import numpy as np
import pandas as pd
import pytest
import ray

from kts.core.backend.address_manager import create_address_manager, get_address_manager
from kts.core.backend.run_manager import RunManager
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.core.frame import KTSFrame
from kts.core.ui import FeatureComputingReport

ray.init(logging_level=0)
try:
    am = get_address_manager()
except:
    am = create_address_manager()

@pytest.fixture
def frame():
    return pd.DataFrame({'a': [1, 2, 3]})

@pytest.fixture
def ktsframe(frame):
    res = KTSFrame(frame)
    res.__meta__['train'] = True
    res.__meta__['fold'] = 'preview'
    return res

@pytest.fixture
def run_manager():
    res = RunManager()
    res.init()
    return res

@pytest.fixture
def report():
    return FeatureComputingReport(None)

@pytest.mark.parametrize('remote', [False, True])
def test_fc_run(ktsframe, run_manager, report, remote):
    am.clear.remote()
    def func(df):
        return df ** 2
    fc = FeatureConstructor(func)
    fc.parallel = remote
    assert len(run_manager.scheduled) == 0
    res = run_manager.run([fc], frame=ktsframe, ret=True, remote=remote, report=report)
    res_frame = res['func']
    run_manager.merge_scheduled()
    assert all(res_frame == ktsframe ** 2)

@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_nested_fc_run(ktsframe, run_manager, report, remote_1, remote_2):
    am.clear.remote()
    def func_1(df):
        return df + 1
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    def func_2(df):
        return func_1(df) ** 2
    func_2 = FeatureConstructor(func_2)
    func_2.parallel = remote_2
    assert len(run_manager.scheduled) == 0
    res = run_manager.run([func_2], frame=ktsframe, ret=True, remote=remote_2, report=report)
    res_frame = res['func_2']
    run_manager.merge_scheduled()
    assert all(res_frame == (ktsframe + 1) ** 2)

@pytest.mark.parametrize('remote_1,remote_2,remote_3', list(product([True, False], repeat=3)) * 1)
def test_deeply_nested_fc_run(ktsframe, run_manager, report, remote_1, remote_2, remote_3):
    am.clear.remote()
    def func_1(df):
        return df + 1
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    def func_2(df):
        return func_1(df) ** 2
    func_2 = FeatureConstructor(func_2)
    func_2.parallel = remote_2
    def func_3(df):
        return func_2(df) ** 2# + func_1(df)
    func_3 = FeatureConstructor(func_3)
    func_3.parallel = remote_3
    assert len(run_manager.scheduled) == 0
    res = run_manager.run([func_3], frame=ktsframe, ret=True, remote=remote_3, report=report)
    res_frame = res['func_3']
    run_manager.merge_scheduled()
    assert all(res_frame == (ktsframe + 1) ** 2 + (ktsframe + 1) ** 4)

@pytest.mark.parametrize('remote_2,remote_3', list(product([True, False], repeat=2)) * 1)
def test_scheduler_cache(ktsframe, run_manager, report, remote_2, remote_3, remote_1=True): # test_runtime_cache
    def func_1(df):
        return df + np.random.randint(1000)
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    def func_2(df):
        return func_1(df)
    func_2 = FeatureConstructor(func_2)
    func_2.parallel = remote_2
    def func_3(df):
        return func_1(df)
    func_3 = FeatureConstructor(func_3)
    func_3.parallel = remote_3
    res = run_manager.run([func_1, func_2, func_3], frame=ktsframe, ret=True, remote=True, report=report)
    assert all(res['func_1'] == res['func_2'])
    assert all(res['func_1'] == res['func_3'])

@pytest.mark.parametrize('remote_1', list(product([True, False], repeat=1)) * 1)
def test_static_cache(ktsframe, run_manager, report, remote_1):
    ktsframe.__meta__['fold'] = 'test_static_cache'
    def func_1(df):
        return df + np.random.randint(1000)
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    res_1 = run_manager.run([func_1], frame=ktsframe, ret=True, remote=True, report=report)['func_1']
    run_manager.merge_scheduled()
    assert len(run_manager.scheduled) == 0
    res_2 = run_manager.run([func_1], frame=ktsframe, ret=True, remote=True, report=report)['func_1']
    assert all(res_1 == res_2)

@pytest.mark.parametrize('remote_1,remote_2,remote_3', list(product([True, False], repeat=3)) * 1)
def test_deeply_nested_fc_scheduler_cache(ktsframe, run_manager, report, remote_1, remote_2, remote_3):
    am.clear.remote()
    def func_1(df):
        return df + np.random.randint(1000)
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    def func_2(df):
        return func_1(df) ** 2
    func_2 = FeatureConstructor(func_2)
    func_2.parallel = remote_2
    def func_3(df):
        return func_2(df) ** 2 + func_1(df)
    func_3 = FeatureConstructor(func_3)
    func_3.parallel = remote_3
    assert len(run_manager.scheduled) == 0
    res = run_manager.run([func_1, func_3], frame=ktsframe, ret=True, remote=remote_3, report=report)
    res_1 = res['func_1']
    res_3 = res['func_3']
    run_manager.merge_scheduled()
    assert all(res_3 == (res_1) ** 2 + (res_1) ** 4)

@pytest.mark.skip
def test_nested_concat():
    pass

@pytest.mark.skip
def test_nested_apply():
    pass

@pytest.mark.skip
def test_nested_generic():
    pass
