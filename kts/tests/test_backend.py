import time
from itertools import product

import numpy as np
import pandas as pd
import pytest
import ray

from kts.core.backend.address_manager import create_address_manager, get_address_manager
from kts.core.backend.run_manager import RunManager
from kts.core.cache import obj_cache, frame_cache
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.core.frame import KTSFrame
from kts.ui.feature_computing_report import FeatureComputingReport

ray.init(logging_level=0)
try:
    am = get_address_manager()
except:
    am = create_address_manager()

@pytest.fixture
def clear_caches():
    for key in obj_cache.ls():
        del obj_cache[key]
    for key in frame_cache.ls():
        del frame_cache[key]

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
def ktsframe_1(frame):
    res = KTSFrame(pd.DataFrame({'a': range(5)}))
    res.__meta__['train'] = True
    res.__meta__['fold'] = 'preview'
    return res

@pytest.fixture
def ktsframe_2(frame):
    res = KTSFrame(pd.DataFrame({'a': range(5, 10)}))
    res.__meta__['train'] = True
    res.__meta__['fold'] = 'preview'
    return res

@pytest.fixture
def run_manager():
    res = RunManager()
    return res

@pytest.fixture
def report():
    return FeatureComputingReport(None)

@pytest.mark.parametrize('remote', [False, True])
def test_fc_run(clear_caches, ktsframe, run_manager, report, remote):
    am.clear.remote()
    def func(df):
        return df ** 2
    fc = FeatureConstructor(func)
    fc.parallel = remote
    assert len(run_manager.scheduled) == 0
    res = run_manager.run([fc], frame=ktsframe, ret=True, report=report)
    res_frame = res['func']
    run_manager.merge_scheduled()
    assert all(res_frame == ktsframe ** 2)

@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_nested_fc_run(clear_caches, ktsframe, run_manager, report, remote_1, remote_2):
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
    res = run_manager.run([func_2], frame=ktsframe, ret=True, report=report)
    res_frame = res['func_2']
    run_manager.merge_scheduled()
    assert all(res_frame == (ktsframe + 1) ** 2)

@pytest.mark.parametrize('remote_1,remote_2,remote_3', list(product([True, False], repeat=3)) * 1)
def test_deeply_nested_fc_run(clear_caches, ktsframe, run_manager, report, remote_1, remote_2, remote_3):
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
    res = run_manager.run([func_3], frame=ktsframe, ret=True, report=report)
    res_frame = res['func_3']
    run_manager.merge_scheduled()
    assert all(res_frame == (ktsframe + 1) ** 2 + (ktsframe + 1) ** 4)

@pytest.mark.parametrize('remote_2,remote_3', list(product([True, False], repeat=2)) * 1)
def test_scheduler_cache(clear_caches, ktsframe, run_manager, report, remote_2, remote_3, remote_1=True):
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
    res = run_manager.run([func_1, func_2, func_3], frame=ktsframe, ret=True, report=report)
    assert all(res['func_1'] == res['func_2'])
    assert all(res['func_1'] == res['func_3'])

@pytest.mark.parametrize('remote_1', [True, False])
def test_static_cache(clear_caches, ktsframe, run_manager, report, remote_1):
    ktsframe.__meta__['fold'] = 'test_static_cache'
    def func_1(df):
        return df + np.random.randint(1000)
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    res_1 = run_manager.run([func_1], frame=ktsframe, ret=True, report=report)['func_1']
    run_manager.merge_scheduled()
    assert len(run_manager.scheduled) == 0
    res_2 = run_manager.run([func_1], frame=ktsframe, ret=True, report=report)['func_1']
    assert all(res_1 == res_2)

@pytest.mark.parametrize('remote_1,remote_2,remote_3', list(product([True, False], repeat=3)) * 1)
def test_deeply_nested_fc_scheduler_cache(clear_caches, ktsframe, run_manager, report, remote_1, remote_2, remote_3):
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
    res = run_manager.run([func_1, func_3], frame=ktsframe, ret=True, report=report)
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

@pytest.mark.parametrize('remote_1', [True, False])
def test_run_cache_columns(clear_caches, ktsframe_1, ktsframe_2, run_manager, report, remote_1):
    ktsframe_1.__meta__['fold'] = 'test_run_cache_columns'
    ktsframe_2.__meta__['fold'] = 'test_run_cache_columns'
    ktsframe_1.__meta__['train'] = True
    ktsframe_2.__meta__['train'] = False

    def func_1(df):
        res = pd.DataFrame({'w': range(len(df))})
        if df.train:
            res['kk'] = 15
        else:
            res['mm'] = 18
        return res
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    res_1 = run_manager.run([func_1], frame=ktsframe_1, ret=True, report=report)['func_1']
    run_manager.merge_scheduled()
    assert all(res_1.columns == ['w', 'kk'])
    assert set(func_1.columns) == set(['w', 'kk'])
    res_2 = run_manager.run([func_1], frame=ktsframe_2, ret=True, report=report)['func_1']
    assert all(res_2.columns == ['w', 'kk'])

@pytest.mark.parametrize('remote_1', [True, False])
def test_run_cache_states(clear_caches, ktsframe_1, ktsframe_2, run_manager, report, remote_1):
    time.sleep(0.001)
    ktsframe_1.__meta__['fold'] = 'test_run_cache_states'
    ktsframe_2.__meta__['fold'] = 'test_run_cache_states'
    ktsframe_1.__meta__['train'] = True
    ktsframe_2.__meta__['train'] = False

    def func_1(df):
        np.random.seed(int(time.time() * 1000000) % 1000000)
        if df.train:
            df.state['tmp'] = np.random.randint(1000)
        tmp = df.state['tmp']
        res = pd.DataFrame(index=df.index)
        res['tmp'] = tmp
        return res
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    res_1 = run_manager.run([func_1], frame=ktsframe_1, ret=True, report=report)['func_1']
    time.sleep(0.001)
    run_manager.merge_scheduled()
    res_2 = run_manager.run([func_1], frame=ktsframe_2, ret=True, report=report)['func_1']
    assert (res_1.tmp.mean() == res_2.tmp.mean())
