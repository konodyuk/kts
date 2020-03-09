import time

import numpy as np
import pandas as pd
import pytest

from kts.core.cache import ObjectCache, FrameCache, CachedMapping


@pytest.mark.parametrize('val_1,val_2', [
    (42, 51),
])
def test_object_cache_timestamp_sync(tmp_path, val_1, val_2):
    c1 = ObjectCache(tmp_path)
    c2 = ObjectCache(tmp_path)
    c1['kek'] = val_1
    assert (c2['kek'] == val_1)
    time.sleep(0.01)
    del c2['kek']
    assert 'kek' not in c1
    c2['kek'] = val_2
    assert (c1['kek'] == val_2)


@pytest.mark.parametrize('val_1,val_2', [
    (pd.DataFrame({'a': [1, 2, 3]}), pd.DataFrame({'b': [4, 5, 6]})),  # fth
    (pd.DataFrame({'a': [1, 2, 3], 'b': [[1], [2], [3]]}).set_index('a'), pd.DataFrame({'b': [4, 5, 6]})),  # pq
    (pd.DataFrame({'a': [1, 2, 3], 'b': [np.array([1]), np.array([2]), np.array([3])]}).set_index('a'), pd.DataFrame({'b': [4, 5, 6]})),  # pq
])
def test_frame_cache_timestamp_sync(tmp_path, val_1, val_2):
    c1 = FrameCache(tmp_path)
    c2 = FrameCache(tmp_path)
    c1['kek'] = val_1
    pd.testing.assert_frame_equal(c2['kek'], val_1)
    time.sleep(0.01)
    del c2['kek']
    assert 'kek' not in c1
    c2['kek'] = val_2
    pd.testing.assert_frame_equal(c1['kek'], val_2)


@pytest.mark.parametrize('val_1', [
    42,
])
def test_object_cache_reload(tmp_path, val_1):
    c1 = ObjectCache(tmp_path)
    c1['kek'] = val_1
    assert c1['kek'] == val_1
    del c1
    c1 = ObjectCache(tmp_path)
    assert 'kek' in c1
    assert c1['kek'] == val_1


@pytest.mark.parametrize('val_1,', [
    pd.DataFrame({'a': [1, 2, 3]}),  # fth
    pd.DataFrame({'a': [1, 2, 3], 'b': [[1], [2], [3]]}).set_index('a'),  # pq
    pd.DataFrame({'a': [1, 2, 3], 'b': [np.array([1]), np.array([2]), np.array([3])]}).set_index('a'),  # pq
])
def test_frame_cache_reload(tmp_path, val_1):
    c1 = FrameCache(tmp_path)
    c1['kek'] = val_1
    pd.testing.assert_frame_equal(c1['kek'], val_1)
    del c1
    c1 = FrameCache(tmp_path)
    assert 'kek' in c1
    pd.testing.assert_frame_equal(c1['kek'], val_1)


def test_cached_mapping():
    cm = CachedMapping('cm_test')
    cm.clear()
    assert repr(cm) == "{\n\t\n}"
    for i in range(5):
        cm[i * 12] = i * 12
    assert repr(cm) == "{\n\t'0': 0\n\t'12': 12\n\t'24': 24\n\t'36': 36\n\t'48': 48\n}"
    del cm[24]
    assert repr(cm) == "{\n\t'0': 0\n\t'12': 12\n\t'36': 36\n\t'48': 48\n}"
    cm[4] = [15]
    assert repr(cm) == "{\n\t'0': 0\n\t'12': 12\n\t'36': 36\n\t'48': 48\n\t'4': [15]\n}"
    del cm
    cm = CachedMapping('cm_test')
    assert repr(cm) == "{\n\t'0': 0\n\t'12': 12\n\t'36': 36\n\t'48': 48\n\t'4': [15]\n}"
    cm.clear()
    assert repr(cm) == '{\n\t\n}'
