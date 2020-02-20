import pytest

from kts.core.cache import ObjectCache, FrameCache


def test_cache_timestamp_sync(tmp_path):
    c1 = ObjectCache(tmp_path)
    c2 = ObjectCache(tmp_path)
    c1['kek'] = 42
    assert c2['kek'] == 42
    del c2['kek']
    assert 'kek' not in c1
    c2['kek'] = 51
    assert c1['kek'] == 51


def test_cache_reload(tmp_path):
    c1 = ObjectCache(tmp_path)
    c1['kek'] = 42
    assert c1['kek'] == 42
    del c1
    c1 = ObjectCache(tmp_path)
    assert 'kek' in c1
    assert c1['kek'] == 42
