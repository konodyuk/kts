import os
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import MutableMapping
from glob import glob
from pathlib import Path
from typing import Any, List
from weakref import WeakValueDictionary

import cloudpickle
import dill
import feather
import numpy as np
import pandas as pd

from kts.core.backend.address_manager import get_address_manager
from kts.core.run_id import RunID


class AlreadyStored(Exception):
    pass


class AbstractCache(ABC):
    extensions = ['.none.none']

    def __init__(self, path=None, weak=False):
        if weak:
            self.data = WeakValueDictionary()
        else:
            self.data = dict()
        self.timestamps = defaultdict(lambda: np.inf)
        if path is not None:
            self.path = Path(path)
        else:
            self.path = None

    @abstractmethod
    def write(self, key: str, value):
        raise NotImplementedError

    @abstractmethod
    def read(self, key: str):
        raise NotImplementedError

    def __contains__(self, key: str):
        return key in self.ls()

    def __getitem__(self, key: str):
        if key not in self:
            raise KeyError(key)
        if self.path is None:
            return self.data[key]
        if key not in self.data or self.timestamps[key] < self.get_timestamp(key):
            self.data[key] = self.read(key)
            self.timestamps[key] = self.get_timestamp(key)
        return self.data[key]

    def __setitem__(self, key: str, value):
        if key in self:
            raise AlreadyStored(key)
        self.data[key] = value
        if self.path is None:
            return
        self.write(key, value)
        self.timestamps[key] = self.get_timestamp(key)

    def __delitem__(self, key: str):
        if key in self.data:
            del self.data[key]
        if self.path is None:
            return
        if key in self.timestamps:
            del self.timestamps[key]
        self.remove_file(key)

    def get_timestamp(self, key: str):
        for ext in self.extensions:
            path = self.path / (key + ext)
            if path.exists():
                return os.path.getmtime(path)

    def remove_file(self, key: str):
        for ext in self.extensions:
            path = self.path / (key + ext)
            if path.exists():
                return os.remove(path)

    def ls(self):
        if self.path is None:
            return list(self.data.keys())
        paths = sum((glob(f'{self.path}/*{ext}') for ext in self.extensions), [])
        paths = sorted(paths, key=os.path.getmtime)
        names = [path[path.rfind('/') + 1:] for path in paths]
        names = [
            name[:name.find(ext)] if ext in name else None
            for ext in self.extensions
            for name in names
        ]
        names = [name for name in names if name is not None]
        return names

    def save(self, value, key: str):
        """backwards compatibility"""
        self[key] = value

    def load(self, key: str):
        """backwards compatibility"""
        return self[key]

    def remove(self, key: str):
        """backwards compatibility"""
        del self[key]


class ObjectCache(AbstractCache):
    extensions = ['.cpkl.obj', '.dill.obj']

    def write(self, key: str, value):
        path = self.path / (key + '.cpkl.obj')
        try:
            cloudpickle.dump(value, open(path, "wb"))
            return
        except:
            os.remove(path)
        path = self.path / (key + '.dill.obj')
        try:
            dill.dump(value, open(path, "wb"))
        except:
            os.remove(path)

    def read(self, key: str):
        path = self.path / (key + '.cpkl.obj')
        if path.exists():
            return cloudpickle.load(open(path, "rb"))
        path = self.path / (key + '.dill.obj')
        return dill.load(open(path, "rb"))


class FrameCache(AbstractCache):
    extensions = ['.fth.frame', '.pq.frame']
    prefix = '_KTS_'

    @staticmethod
    def has_default_index(df: pd.DataFrame):
        if not isinstance(df.index, pd.RangeIndex):
            return False
        if not (df.index.start == 0 and df.index.stop == df.shape[0]):
            return False
        return True

    def recover_index(self, df: pd.DataFrame):
        for col in df.columns:
            if col.startswith(self.prefix):
                df.set_index(col, inplace=True)
                df.index.name = df.index.name[len(self.prefix):]
                if df.index.name == 'UNTITLED_INDEX':
                    df.index.name = None
                return

    def write(self, key: str, df: pd.DataFrame):
        had_default_index = self.has_default_index(df)
        if not had_default_index:
            if df.index.name is None:
                df.index.name = 'UNTITLED_INDEX'
            df.index.name = self.prefix + df.index.name
            df.reset_index(inplace=True)
        try:
            path = self.path / (key + '.fth.frame')
            feather.write_dataframe(df, path)
        except:
            os.remove(path)
            path = self.path / (key + '.pq.frame')
            df.to_parquet(path)
        finally:
            self.recover_index(df)

    def read(self, key: str):
        path = self.path / (key + '.fth.frame')
        if path.exists():
            df = feather.read_dataframe(path, use_threads=True)
            self.recover_index(df)
            return df
        else:
            path = self.path / (key + '.pq.frame')
            df = pd.read_parquet(path)
            self.recover_index(df)
            return df

    def save_run(self, value: pd.DataFrame, run_id: RunID):
        self[run_id.get_alias_name()] = value

    def load_run(self, run_id: RunID):
        return self[run_id.get_alias_name()]

    def has_run(self, run_id: RunID):
        return run_id.get_alias_name() in self

    def list_runs(self) -> List[RunID]:
        result = list()
        for key in self.ls():
            try:
                run_id = RunID.from_alias_name(key)
                result.append(run_id)
            except:
                pass
        return result

    def del_run(self, run_id: RunID):
        del self[run_id.get_alias_name()]


obj_cache = ObjectCache()
frame_cache = FrameCache()


class CachedMapping(MutableMapping):
    def __init__(self, name: str, cache: AbstractCache = obj_cache):
        self.name = name
        self.cache = cache

    def __create_name__(self, key):
        return f"M.{self.name}.{key}"

    def __from_name__(self, name):
        return name[len(self.__create_name__('')):]

    def __len__(self):
        return len(self.__keys__())

    def __getitem__(self, key: str):
        return self.cache.load(self.__create_name__(key))

    def __setitem__(self, key: str, value: Any):
        self.cache.save(value, self.__create_name__(key))

    def __delitem__(self, key: str):
        self.cache.remove(self.__create_name__(key))

    def __keys__(self):
        return [self.__from_name__(i) for i in self.cache.ls() if i.startswith(self.__create_name__(''))]

    def __iter__(self):
        return self.__keys__().__iter__()

    def __repr__(self):
        return "{\n\t" + '\n\t'.join([f"'{key}': {value}" for key, value in self.items()]) + '\n}'

    def __reduce__(self):
        """This does not consider case of using frame_cache as backend, as user_cache_frame is the only case"""
        return (self.__class__, (self.name,))


user_cache_frame = CachedMapping('user_cache_frame', frame_cache)
user_cache_obj = CachedMapping('user_cache_obj', obj_cache)

def save(value, key: str):
    assert key not in user_cache_frame, f'{key} is already saved. Load it with kts.load("{key}") or remove with kts.rm("{key}")'
    assert key not in user_cache_obj, f'{key} is already saved. Load it with kts.load("{key}") or remove with kts.rm("{key}")'
    if isinstance(value, pd.DataFrame):
        user_cache_frame[key] = value
    else:
        user_cache_obj[key] = value

def load(key: str):
    if key in user_cache_frame:
        return user_cache_frame[key]
    if key in user_cache_obj:
        return user_cache_obj[key]
    raise KeyError

def rm(key: str):
    if key in user_cache_frame:
        del user_cache_frame[key]
    if key in user_cache_obj:
        del user_cache_obj[key]
    am = get_address_manager()
    am.delete.remote(key)

def ls():
    return list(user_cache_obj) + list(user_cache_frame)
