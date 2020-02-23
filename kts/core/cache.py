import os
from abc import ABC, abstractmethod
from glob import glob
from pathlib import Path
from weakref import WeakValueDictionary

import dill
import feather
import pandas as pd


class AlreadyStored(Exception):
    pass


class AbstractCache(ABC):
    extensions = ['.none.none']

    def __init__(self, path=None, weak=False):
        if weak:
            self.data = WeakValueDictionary()
        else:
            self.data = dict()
        self.timestamps = dict()
        if path is not None:
            self.path = Path(path)
        else:
            self.path = None

    @abstractmethod
    def write(self, key, value):
        raise NotImplemented

    @abstractmethod
    def read(self, key):
        raise NotImplemented

    def __contains__(self, key):
        return key in self.ls()

    def __getitem__(self, key):
        if key not in self:
            raise KeyError(key)
        if self.path is None:
            return self.data[key]
        if key not in self.data or self.timestamps[key] < self.get_timestamp(key):
            self.data[key] = self.read(key)
            self.timestamps[key] = self.get_timestamp(key)
        return self.data[key]

    def __setitem__(self, key, value):
        if key in self:
            raise AlreadyStored(key)
        self.data[key] = value
        if self.path is None:
            return
        self.write(key, value)
        self.timestamps[key] = self.get_timestamp(key)

    def __delitem__(self, key):
        if key in self.data:
            del self.data[key]
        if self.path is None:
            return
        if key in self.timestamps:
            del self.timestamps[key]
        self.remove_file(key)

    def get_timestamp(self, key):
        for ext in self.extensions:
            path = self.path / (key + ext)
            if path.exists():
                return os.path.getmtime(path)

    def remove_file(self, key):
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

    def save(self, value, key):
        """backwards compatibility"""
        self[key] = value

    def load(self, key):
        """backwards compatibility"""
        return self[key]

    def remove(self, key):
        """backwards compatibility"""
        del self[key]


class ObjectCache(AbstractCache):
    extensions = ['.dill.obj']

    def write(self, key, value):
        path = self.path / (key + '.dill.obj')
        try:
            dill.dump(value, open(path, "wb"))
        except dill.PicklingError:
            if path.exists():
                os.remove(path)

    def read(self, key):
        path = self.path / (key + '.dill.obj')
        return dill.load(open(path, "rb"))


class FrameCache(AbstractCache):
    extensions = ['.fth.frame', '.pq.frame']
    prefix = '_KTS_'

    @staticmethod
    def has_default_index(df):
        if not isinstance(df.index, pd.RangeIndex):
            return False
        if not (df.index.start == 0 and df.index.stop == df.shape[0]):
            return False
        return True

    def recover_index(self, df):
        for col in df.columns:
            if col.startswith(self.prefix):
                df.set_index(col, inplace=True)
                df.index.name = df.index.name[len(self.prefix):]
                if df.index.name == 'UNTITLED_INDEX':
                    df.index.name = None
                return

    def write(self, key, df):
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
        self.recover_index(df)

    def read(self, key):
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

    def save_run(self, value, run_id):
        self[run_id.get_alias_name()] = value

    def load_run(self, run_id):
        return self[run_id.get_alias_name()]


obj_cache = ObjectCache()
frame_cache = FrameCache()
