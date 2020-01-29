from collections import OrderedDict
from copy import deepcopy
import pytest
import numpy as np
import pandas as pd

from kts.core.cache import CachedMapping, CachedList, frame_cache, RunID, base_frame_cache, obj_cache


@pytest.fixture
def empty_ram_cache():
    obj_cache.memory.clear()
    base_frame_cache.memory.clear()


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


def test_cached_list():
    cl = CachedList('cl_test')
    cl.clear()
    assert repr(cl) == "[]"
    for i in range(5):
        cl.append(i * 12)
    assert repr(cl) == "[0, 12, 24, 36, 48]"
    del cl[1]
    assert repr(cl) == "[0, 24, 36, 48]"
    for i in range(5):
        cl.append(i * 12)
    assert repr(cl) == "[0, 24, 36, 48, 0, 12, 24, 36, 48]"
    cl[4] = [15]
    assert repr(cl) == "[0, 24, 36, 48, [15], 12, 24, 36, 48]"
    cl.pop()
    assert repr(cl) == "[0, 24, 36, 48, [15], 12, 24, 36]"
    del cl
    cl = CachedList('cl_test')
    assert repr(cl) == "[0, 24, 36, 48, [15], 12, 24, 36]"
    cl.clear()
    assert repr(cl) == "[]"


def test_frame_cache():
    df = pd.DataFrame({'a': [1, 2], 'b': list('cd')})
    frame_cache.save(df, 'test', False)
    assert all(frame_cache.load('test') == df)
    df1 = deepcopy(df)
    df1['c'] = 0
    tmp = deepcopy(df1)
    frame_cache.save(df1, 'test_del', True)
    assert df1.empty
    assert all(frame_cache.load('test_del') == tmp)
    assert frame_cache.aliases['test'].column_references == \
           OrderedDict([('a', 'a__KTS__test'), ('b', 'b__KTS__test')])
    assert frame_cache.aliases['test_del'].column_references == \
           OrderedDict([('a', 'a__KTS__test_del'), ('b', 'b__KTS__test_del'), ('c', 'c__KTS__test_del')])
    first_iblock = list(frame_cache.index_blocks.keys())[0]
    print(frame_cache.index_blocks[first_iblock].index)
    df2 = pd.DataFrame({'w': [2, 2, 2], 'z': list('gpt')}).set_index('z')
    print(df2)
    frame_cache.save(df2, 'df2', False)
    second_iblock = list(set(frame_cache.index_blocks.keys()) - {first_iblock})[0]
    print(frame_cache.index_blocks[second_iblock].data)
    df3 = pd.DataFrame({'w': [2, 2], 'z': list('gt')}).set_index('z')
    df4 = pd.DataFrame({'w': [8], 'z': list('p')}).set_index('z')
    df5 = pd.DataFrame({'k': [7, 7, 7], 'z': list('gpt')}).set_index('z')
    frame_cache.save(df3, 'df3', destroy=False)
    frame_cache.save(df4, 'df4', destroy=False)
    assert len(frame_cache.index_blocks[second_iblock].queue_to_merge) == 0
    frame_cache.save(df5, 'df5', destroy=True)
    assert len(frame_cache.index_blocks[second_iblock].queue_to_merge) == 0  # lazy is always False for .save
    assert df5.empty
    assert not df3.empty
    assert not df4.empty
    assert all((frame_cache.index_blocks[second_iblock].data == pd.DataFrame({
        'z': list('gpt'),
        'w__KTS__df2': [2, 2, 2],
        'w__KTS__df3': [2., np.nan, 2.],
        'w__KTS__df4': [np.nan, 8., np.nan],
        'k__KTS__df5': [7, 7, 7]
    }).set_index('z')) | frame_cache.index_blocks[second_iblock].data.isna())
    df6 = pd.DataFrame({'w': [6, 6], 'z': list('gt')}).set_index('z')
    df7 = pd.DataFrame({'w': [9], 'z': list('p')}).set_index('z')
    tmp6 = deepcopy(df6)
    tmp7 = deepcopy(df7)
    rid6 = RunID('test', 'samehash', 'differenthash1')
    rid7 = RunID('test', 'samehash', 'differenthash2')
    frame_cache.save_run(df6, rid6, lazy=True)
    frame_cache.save_run(df7, rid7, lazy=True)
    assert len(frame_cache.index_blocks[second_iblock].queue_to_merge) == 2
    assert not df6.empty
    assert not df7.empty
    print(frame_cache.index_blocks[second_iblock].queue_to_merge)
    assert all((frame_cache.index_blocks[second_iblock].data == pd.DataFrame({
        'z': list('gpt'),
        'w__KTS__df2': [2, 2, 2],
        'w__KTS__df3': [2., np.nan, 2.],
        'w__KTS__df4': [np.nan, 8., np.nan],
        'k__KTS__df5': [7, 7, 7],
        'w__KTS__test__KTS__samehash': [6., 9., 6.]
    }).set_index('z')) | frame_cache.index_blocks[second_iblock].data.isna())
    assert len(frame_cache.index_blocks[second_iblock].queue_to_merge) == 0
    assert df6.empty
    assert df7.empty
    print(frame_cache.index_blocks[second_iblock].data)
    assert all(frame_cache.load_run(rid6).data == tmp6)
    assert all(frame_cache.load_run(rid7).data == tmp7)
    print(obj_cache.memory.keys())
    print(frame_cache.aliases)
    print(frame_cache.indices)
    df8 = pd.DataFrame({'p': [13, 13, 13], 'z': list('gpt')}).set_index('z')
    # df9 = pd.DataFrame({'p': [9], 'z': list('p')}).set_index('z')
    rid8 = RunID('other_function', '000000', '812dfhsd')
    # rid9 = RunID('test', 'samehash', 'differenthash2')
    frame_cache.save_run(df8, rid8, lazy=True)
    # frame_cache.save_run(df7, rid7, lazy=True)
    print(frame_cache.load_run(rid6).data)
    print(frame_cache.load_run(rid8).data)
    assert all((frame_cache.load_run(rid6) + frame_cache.load_run(rid8)).data == pd.DataFrame({
        'z': list('gt'),
        'w': [6., 6.],
        'p': [13, 13],
    }).set_index('z'))
    assert all((frame_cache.load_run(rid7) + frame_cache.load_run(rid8)).data == pd.DataFrame({
        'z': list('p'),
        'w': [9.],
        'p': [13],
    }).set_index('z'))
    # assert 0
