from itertools import product

import pandas as pd
import pytest

from kts.core.backend.run_manager import run_manager
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.core.feature_set import FeatureSet
from kts.settings import cfg


@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_no_stateful(frame, folds, clear_caches, report, remote_1, remote_2):
    def simple_feature(df):
        return df[['int']] * 2
    simple_feature = FeatureConstructor(simple_feature)
    simple_feature.parallel = remote_1

    def another_simple_feature(df):
        return df[['int_rand']] * 2
    another_simple_feature = FeatureConstructor(another_simple_feature)
    another_simple_feature.parallel = remote_2

    fs = FeatureSet([simple_feature, another_simple_feature], train_frame=frame, targets='float_rand')
    cfs = fs.split(folds)
    cfs.compute(report=report)
    assert len(run_manager.scheduled) == 0
    for i, (idx_train, idx_valid) in enumerate(folds):
        assert cfs.fold(i).train.shape[0] == idx_train.shape[0]
        assert cfs.fold(i).valid.shape[0] == idx_valid.shape[0]
    assert len(run_manager.scheduled) == 0


@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_cv_feature_set(frame, folds, clear_caches, report, remote_1, remote_2):
    def simple_feature(df):
        return df[['int']]  * 2
    simple_feature = FeatureConstructor(simple_feature)
    simple_feature.parallel = remote_1

    def stateful_feature(df):
        res = pd.DataFrame(index=df.index)
        if df.train:
            df.state['mean'] = df['int'].mean()
        mean = df.state['mean']
        res['mean'] = mean
        return res
    stateful_feature = FeatureConstructor(stateful_feature)
    stateful_feature.parallel = remote_2

    fs = FeatureSet([simple_feature], [stateful_feature], train_frame=frame, targets='float_rand')
    cfs = fs.split(folds)
    cfs.compute(report=report)
    assert len(run_manager.scheduled) == 0

    for i in range(len(folds)):
        train_mean = cfs.fold(i).train[:, 1]
        valid_mean = cfs.fold(i).valid[:, 1]
        assert len(set(train_mean)) == 1
        assert len(set(valid_mean)) == 1
        assert train_mean.mean() == pytest.approx(valid_mean.mean())
    assert len(run_manager.scheduled) == 0


@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_computed_target(frame, folds, clear_caches, report, remote_1, remote_2):
    cfg.preview_mode = False
    def target_feature_1(df):
        res = pd.DataFrame(index=df.index)
        res['target_1'] = df[['int']] * 2
        return res
    target_feature_1 = FeatureConstructor(target_feature_1)
    target_feature_1.parallel = remote_1

    def target_feature_2(df):
        res = pd.DataFrame(index=df.index)
        res['target_2'] = df[['float_rand']] ** 2
        return res
    target_feature_2 = FeatureConstructor(target_feature_2)
    target_feature_2.parallel = remote_2

    fs = FeatureSet([target_feature_1, target_feature_2], train_frame=frame, targets=['target_1', 'target_2'])
    cfs = fs.split(folds)
    cfs.compute(report=report)
    assert len(run_manager.scheduled) == 0

    for i, (idx_train, idx_valid) in enumerate(folds):
        train_target = cfs.fold(i).train_target
        valid_target = cfs.fold(i).valid_target

        assert train_target[:, 0] == pytest.approx(frame['int'].values[idx_train] * 2)
        assert train_target[:, 1] == pytest.approx(frame['float_rand'].values[idx_train] ** 2)
        assert valid_target[:, 0] == pytest.approx(frame['int'].values[idx_valid] * 2)
        assert valid_target[:, 1] == pytest.approx(frame['float_rand'].values[idx_valid] ** 2)
    assert len(run_manager.scheduled) == 0

@pytest.mark.parametrize('remote_1,remote_2', product([True, False], repeat=2))
def test_auxiliary(frame, clear_caches, report, remote_1, remote_2):
    cfg.preview_mode = False
    def aux_feature_1(df):
        res = pd.DataFrame(index=df.index)
        res['aux_1'] = df[['int']] * 2
        return res
    aux_feature_1 = FeatureConstructor(aux_feature_1)
    aux_feature_1.parallel = remote_1

    def aux_feature_2(df):
        res = pd.DataFrame(index=df.index)
        res['aux_2'] = df[['float_rand']] ** 2
        return res
    aux_feature_2 = FeatureConstructor(aux_feature_2)
    aux_feature_2.parallel = remote_2

    fs = FeatureSet([aux_feature_1, aux_feature_2],
                    train_frame=frame,
                    targets=['int'],
                    auxiliary=['aux_1', 'aux_2', 'str'])
    fs.compute()
    assert len(run_manager.scheduled) == 0

    aux = fs.aux

    assert (aux['aux_1'].values == pytest.approx(frame['int'].values * 2))
    assert (aux['aux_2'].values == pytest.approx(frame['float_rand'].values ** 2))
    pd.testing.assert_series_equal(aux['str'], frame['str'])

    assert len(run_manager.scheduled) == 0

@pytest.mark.skip
def test_not_cached_feature():
    pass

@pytest.mark.skip
def test_inference():
    pass
