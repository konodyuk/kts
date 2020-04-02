import time
from itertools import product

import numpy as np
import pandas as pd
import pytest

from kts.core.backend.address_manager import get_address_manager
from kts.core.backend.progress import pbar
from kts.core.backend.signal import get_signal_manager
from kts.core.feature_constructor.user_defined import FeatureConstructor


am = get_address_manager()
sm = get_signal_manager()


@pytest.mark.parametrize('remote', [False, True])
def test_fc_run(clear_caches, int_frame, run_manager, report, remote):
    am.clear.remote()
    def func(df):
        return df ** 2
    fc = FeatureConstructor(func)
    fc.parallel = remote
    assert len(run_manager.scheduled) == 0
    res = run_manager.run([fc], frame=int_frame, train=True, fold='preview', ret=True, report=report)
    res_frame = res['func']
    run_manager.merge_scheduled()
    assert all(res_frame == int_frame ** 2)

@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_nested_fc_run(clear_caches, int_frame, run_manager, report, remote_1, remote_2):
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
    res = run_manager.run([func_2], frame=int_frame, train=True, fold='preview', ret=True, report=report)
    res_frame = res['func_2']
    run_manager.merge_scheduled()
    assert all(res_frame == (int_frame + 1) ** 2)

@pytest.mark.parametrize('remote_1,remote_2,remote_3', list(product([True, False], repeat=3)) * 1)
def test_deeply_nested_fc_run(clear_caches, int_frame, run_manager, report, remote_1, remote_2, remote_3):
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
    res = run_manager.run([func_3], frame=int_frame, train=True, fold='preview', ret=True, report=report)
    res_frame = res['func_3']
    run_manager.merge_scheduled()
    assert all(res_frame == (int_frame + 1) ** 2 + (int_frame + 1) ** 4)

@pytest.mark.parametrize('remote_2,remote_3', list(product([True, False], repeat=2)) * 1)
def test_scheduler_cache(clear_caches, int_frame, run_manager, report, remote_2, remote_3, remote_1=True):
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
    res = run_manager.run([func_1, func_2, func_3], frame=int_frame, train=True, fold='preview', ret=True, report=report)
    assert all(res['func_1'] == res['func_2'])
    assert all(res['func_1'] == res['func_3'])

@pytest.mark.parametrize('remote_1', [True, False])
def test_static_cache(clear_caches, int_frame, run_manager, report, remote_1):
    def func_1(df):
        return df + np.random.randint(1000)
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    res_1 = run_manager.run([func_1], frame=int_frame, train=True, fold='test_static_cache', ret=True, report=report)['func_1']
    run_manager.merge_scheduled()
    assert len(run_manager.scheduled) == 0
    res_2 = run_manager.run([func_1], frame=int_frame, train=True, fold='test_static_cache', ret=True, report=report)['func_1']
    assert all(res_1 == res_2)

@pytest.mark.parametrize('remote_1,remote_2,remote_3', list(product([True, False], repeat=3)) * 1)
def test_deeply_nested_fc_scheduler_cache(clear_caches, int_frame, run_manager, report, remote_1, remote_2, remote_3):
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
    res = run_manager.run([func_1, func_3], frame=int_frame, train=True, fold='preview', ret=True, report=report)
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

@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_nested_progressbar(clear_caches, int_frame, run_manager, report, remote_1, remote_2):
    am.clear.remote()
    def func_1(df):
        for _ in pbar(range(10)):
            time.sleep(0.1)
        return df + 1
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    def func_2(df):
        for _ in pbar(range(10), title='test'):
            time.sleep(0.1)
        return func_1(df) ** 2
    func_2 = FeatureConstructor(func_2)
    func_2.parallel = remote_2
    assert len(run_manager.scheduled) == 0
    res = run_manager.run([func_2], frame=int_frame, train=True, fold='preview', ret=True, report=report)
    res_frame = res['func_2']
    run_manager.merge_scheduled()
    assert all(res_frame == (int_frame + 1) ** 2)


@pytest.mark.parametrize('remote_1', [True, False])
def test_run_cache_columns(clear_caches, int_frame, other_int_frame, run_manager, report, remote_1):
    def func_1(df):
        res = pd.DataFrame({'w': range(len(df))})
        if df.train:
            res['kk'] = 15
        else:
            res['mm'] = 18
        return res
    func_1 = FeatureConstructor(func_1)
    func_1.parallel = remote_1
    res_1 = run_manager.run([func_1], frame=int_frame, train=True, fold='test_run_cache_columns', ret=True, report=report)['func_1']
    run_manager.merge_scheduled()
    assert all(res_1.columns == ['w', 'kk'])
    assert set(func_1.columns) == set(['w', 'kk'])
    res_2 = run_manager.run([func_1], frame=other_int_frame, train=False, fold='test_run_cache_columns', ret=True, report=report)['func_1']
    assert all(res_2.columns == ['w', 'kk'])

@pytest.mark.parametrize('remote_1', [True, False])
def test_run_cache_states(clear_caches, int_frame, other_int_frame, run_manager, report, remote_1):
    time.sleep(0.001)
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
    res_1 = run_manager.run([func_1], frame=int_frame, train=True, fold="test_run_cache_states", ret=True, report=report)['func_1']
    time.sleep(0.001)
    run_manager.merge_scheduled()
    res_2 = run_manager.run([func_1], frame=other_int_frame, train=False, fold="test_run_cache_states", ret=True, report=report)['func_1']
    assert (res_1.tmp.mean() == res_2.tmp.mean())
