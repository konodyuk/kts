from collections import MutableMapping, MutableSequence
from collections import OrderedDict
from copy import copy
from hashlib import sha256
from itertools import chain
from typing import List, Union, Dict, Callable, Set, Optional, Tuple, Any, Collection

import pandas as pd

from kts.core.frame import KTSFrame
from kts.util.hashing import hash_frame

AnyFrame = Union[pd.DataFrame, KTSFrame]

SPLITTER = '__KTS__'


def hash_index(index):
    return sha256(index.values).hexdigest()


def destroy_frame(df):
    import gc
    df.drop(df.index, inplace=True)
    df.drop(df.columns, axis=1, inplace=True)
    gc.collect()


class AliasJoiningError(Exception):
    pass


class ObjectCache:
    def __init__(self):
        self.memory = {}

    def save(self, obj, name: str):
        self.memory[name] = obj

    def remove(self, name):
        self.memory.pop(name)

    def load(self, name: str):
        return self.memory[name]

    def __contains__(self, name: str):
        return name in self.memory

    def ls(self):
        return list(self.memory.keys())

obj_cache = ObjectCache()


class BaseFrameCache:
    """Should also synchronize on-disk and in-memory copies of objects."""

    def __init__(self):
        self.memory = {}

    def save(self, obj, name: str):
        self.memory[name] = obj

    def load(self, name: str):
        return self.memory[name]

    def __contains__(self, name: str):
        return name in self.memory

base_frame_cache = BaseFrameCache()


class CachedMapping(MutableMapping):
    def __init__(self, name):
        self.name = name

    def __create_name__(self, key):
        return f"M_{self.name}{SPLITTER}{key}"

    def __from_name__(self, name):
        return name[len(self.__create_name__('')):]

    def __len__(self):
        return len(self.__keys__())

    def __getitem__(self, key: str):
        return obj_cache.load(self.__create_name__(key))

    def __setitem__(self, key: str, value: Any):
        obj_cache.save(value, self.__create_name__(key))

    def __delitem__(self, key: str):
        obj_cache.remove(self.__create_name__(key))

    def __keys__(self):
        return [self.__from_name__(i) for i in obj_cache.ls() if i.startswith(self.__create_name__(''))]

    def __iter__(self):
        return self.__keys__().__iter__()

    def __repr__(self):
        return "{\n\t" + '\n\t'.join([f"'{key}': {value}" for key, value in self.items()]) + '\n}'


class CachedList(MutableSequence):
    def __init__(self, name):
        self.name = name
        self.cm = CachedMapping(f"L_{self.name}")

    @property
    def idx2key(self):
        if "idx2key" not in self.cm:
            self.cm["idx2key"] = list()
        return self.cm["idx2key"]

    @idx2key.setter
    def idx2key(self, value):
        self.cm["idx2key"] = value

    def __len__(self):
        return len(self.cm) - 1

    def __setitem__(self, key, value):
        cm_key = self.idx2key[key]
        self.cm[cm_key] = value

    def __getitem__(self, key):
        cm_key = self.idx2key[key]
        return self.cm[cm_key]

    def __new_key__(self):
        i = 0
        while i in self.cm:
            i += 1
        return i

    def insert(self, key, value):
        cm_key = self.__new_key__()
        self.cm[cm_key] = value
        self.idx2key.insert(key, cm_key)

    def __delitem__(self, key):
        cm_key = self.idx2key[key]
        del self.cm[cm_key]
        del self.idx2key[key]

    def __repr__(self):
        return '[' + ", ".join([str(i) for i in self]) + ']'


class DataFrameAlias:
    def __init__(self,
                 index_name: str,
                 column_references: OrderedDict,
                 index_block_name: str,
                 name: Optional[str] = None,
                 dependencies: Optional[Set[str]] = None,
                 frame_hash: Optional[str] = None):
        self.index_name = index_name
        self.column_references = column_references
        self.index_block_name = index_block_name
        self.name = name
        self.dependencies = dependencies
        self.hash = frame_hash
        self.is_loaded = False
        if self.name not in self.index_block.alias_names:
            self.index_block.alias_names.append(self.name)

    def load(self, lazy: bool = False):
        if self.is_loaded:
            return
        if self.dependencies is None:
            df = base_frame_cache.load(self.name)
            self.index_block.insert(df, destroy=True, lazy=lazy)
        else:
            for alias_name in self.dependencies:
                frame_cache.aliases[alias_name].load(lazy=True)
        self.is_loaded = True

    @property
    def data(self) -> pd.DataFrame:
        block = self.index_block
        self.load()
        res = block.data.loc[self.index, self.internal_columns]
        res.columns = self.columns
        return res

    @property
    def columns(self) -> List[str]:
        """Works as pd.DataFrame().columns"""
        return list(self.column_references.keys())

    @property
    def internal_columns(self) -> List[str]:
        """Returns real columns"""
        return list(self.column_references.values())

    @property
    def index(self) -> pd.Index:
        return frame_cache.indices[self.index_name]

    @property
    def index_block(self) -> 'IndexBlock':
        return frame_cache.index_blocks[self.index_block_name]

    @property
    def is_loaded(self) -> bool:
        return self.index_block.is_alias_loaded[self.name]

    @is_loaded.setter
    def is_loaded(self, value: bool):
        self.index_block.is_alias_loaded[self.name] = value

    def join(self, other: 'DataFrameAlias') -> 'DataFrameAlias':
        if self.index_block_name != other.index_block_name:
            raise AliasJoiningError('Can only join aliases from the same index block')
        new_index = self.index.intersection(other.index)
        new_index_name = frame_cache._get_index_reference(new_index)
        new_column_references = OrderedDict(chain(self.column_references.items(), other.column_references.items()))
        new_dependencies = set()
        if self.dependencies is None:
            new_dependencies |= {self.name}
        else:
            new_dependencies |= self.dependencies
        if other.dependencies is None:
            new_dependencies |= {other.name}
        else:
            new_dependencies |= other.dependencies
        return DataFrameAlias(index_name=new_index_name,
                              column_references=new_column_references,
                              index_block_name=self.index_block_name,
                              dependencies=new_dependencies)

    def __sub__(self, cols: List[str]) -> 'DataFrameAlias':
        """Returns a new DataFrameAlias without passed columns.

        Preserves order in the instance."""
        new_column_references = copy(self.column_references)
        for col in cols:
            new_column_references.pop(col, None)
        new_dependencies = self.dependencies
        if new_dependencies is not None:
            to_remove = set()
            for alias_name in new_dependencies:
                alias = frame_cache.aliases[alias_name]
                if len(set(alias.columns) & set(new_column_references.keys())) == 0:
                    to_remove.add(alias_name)
            new_dependencies -= to_remove
        return DataFrameAlias(index_name=self.index_name,
                              column_references=new_column_references,
                              index_block_name=self.index_block_name,
                              name=self.name,
                              dependencies=new_dependencies)

    def __and__(self, cols: List[str]) -> 'DataFrameAlias':
        """Returns a new DataFrameAlias with only passed columns.

        Preserves order in the instance."""
        cols_to_remove = list(set(self.columns) - set(cols))
        return self - cols_to_remove

    def __add__(self, other: 'DataFrameAlias') -> 'DataFrameAlias':
        return self.join(other)


class RunID:
    def __init__(self, function_name: str, fold: str, input_frame: Optional[str] = None):
        self.function_name = function_name
        self.fold = fold
        self.input_frame = input_frame

    @staticmethod
    def from_column_name(column_name: str) -> 'RunID':
        args = column_name.split(SPLITTER)[1:]
        return RunID(*args)

    @staticmethod
    def from_alias_name(alias_name: str) -> 'RunID':
        args = alias_name.split(SPLITTER)
        return RunID(*args)

    def get_column_name(self, column_name: str) -> str:
        return f"{column_name}{SPLITTER}{self.function_name}{SPLITTER}{self.fold}"

    def get_alias_name(self) -> str:
        return f"{self.function_name}{SPLITTER}{self.fold}{SPLITTER}{self.input_frame}"

    def get_state_name(self) -> str:
        return f"{self.function_name}{SPLITTER}{self.fold}"

    def get_state_key(self) -> Tuple[str, str]:
        return self.function_name, self.fold

    @property
    def state_id(self) -> Tuple[str, str]:
        return self.get_state_key()

    def __eq__(self, other):
        return (self.function_name == other.function_name
                and self.fold == other.fold
                and self.input_frame == other.input_frame)

    def __hash__(self):
        return hash(self.get_alias_name())


class IndexBlock:
    def __init__(self, index_name: str, alias_names: List[str], name: str):
        self.index_name = index_name
        self._data = pd.DataFrame(index=frame_cache.indices[index_name])
        self.alias_names = alias_names
        self.is_alias_loaded = {i: False for i in self.alias_names}
        self.name = name
        self.queue_to_merge = []

    def contains(self, index: pd.Index) -> bool:
        return len(self.index.intersection(index)) == len(index)

    def consolidate(self):
        if len(self.queue_to_merge) == 0:
            return
        for flag, df in self.queue_to_merge:
            for col in df.columns:
                if col not in self._data.columns:
                    self._data[col] = df[col]
                else:
                    self._data.loc[df.index, col] = df[col]
        self._data._data = self._data._data.consolidate()
        for flag, df in self.queue_to_merge:
            if flag:
                destroy_frame(df)
        self.queue_to_merge.clear()

    def insert(self, frame, destroy: bool = True, lazy: bool = True):
        self.queue_to_merge.append((destroy, frame))
        if not lazy:
            self.consolidate()

    @property
    def data(self) -> pd.DataFrame:
        self.consolidate()
        return self._data

    @property
    def aliases(self) -> List[DataFrameAlias]:
        return [frame_cache.aliases[i] for i in self.alias_names]

    @property
    def index(self) -> pd.Index:
        return frame_cache.indices[self.index_name]

    def to_dict(self) -> Dict[str, str]:
        return {'index_name': self.index_name,
                'alias_names': self.alias_names,
                'name': self.name}


class FrameCache:
    def __init__(self):
        self.aliases = CachedMapping('aliases')
        self.indices = CachedMapping('indices')
        self.index_blocks_encoded = CachedMapping('index_blocks')
        self.index_blocks = {k: IndexBlock(**v) for k, v in
                             self.index_blocks_encoded.items()}

    def save(self, df: AnyFrame, name: Optional[str] = None, destroy: bool = False) -> Optional[str]:
        """Never uses lazy insertion, as given dataframe is not internal and may change after saving operation."""
        if name is None:
            frame_hash = hash_frame(df)
            for alias in self.aliases.items():
                if alias.hash == frame_hash:
                    return alias.name
            name = frame_hash
        create_reference = lambda col: f"{col}{SPLITTER}{name}"
        self._save(df=df, name=name, create_reference=create_reference, destroy=destroy, lazy=False)
        if name is None:
            return name

    def load(self, name: str, columns: Optional[List[str]] = None, index: Optional[Collection[int]] = None, ret_alias: bool = False) -> AnyFrame:
        """
        Index parameter is unsafe and is for internal use only.
        It is assumed that max(index) is less than len(alias.index), and no checks is made.
        Only values yielded from sklearn splitters are to be passed as index.
        """
        alias = self.aliases[name]
        if columns is not None:
            alias = alias & columns
        if index is not None:
            pd_index = alias.index.iloc[index]
            index_reference = self._get_index_reference(pd_index)
            alias.index_name = index_reference
        if ret_alias:
            return alias
        else:
            return alias.data

    def save_run(self, df: AnyFrame, run_id: RunID, lazy: bool = True):
        create_reference = run_id.get_column_name
        name = run_id.get_alias_name()
        self._save(df=df, name=name, create_reference=create_reference, destroy=True, lazy=lazy)

    def load_run(self, run_id: RunID) -> DataFrameAlias:
        return self.aliases[run_id.get_alias_name()]

    def _save(self, df: AnyFrame, name: str, create_reference: Callable[[str], str], destroy: bool, lazy: bool):
        assert name not in self.aliases, f"{name} is already saved"
        index_block_name = self._get_index_block(df.index)
        index_name = self._get_index_reference(df.index)
        column_references = OrderedDict([(col, create_reference(col)) for col in df.columns])
        frame_hash = hash_frame(df)
        self.aliases[name] = DataFrameAlias(index_name=index_name,
                                            column_references=column_references,
                                            index_block_name=index_block_name,
                                            name=name,
                                            frame_hash=frame_hash)
        if destroy:
            df_to_save = df  # change columns of the dataframe itself
        else:
            df_to_save = df.copy(deep=False)  # change columns of its copy
        df_to_save.columns = list(column_references.values())
        self.index_blocks[index_block_name].insert(df_to_save, destroy=destroy, lazy=lazy)
        base_frame_cache.save(df_to_save, name)

    def __contains__(self, name: str) -> bool:
        return name in self.aliases

    def _get_index_block(self, index: pd.Index) -> str:
        """Gets the smallest index block index of which contains index of `df`.

        Returns IndexBlock instance (existing or new)."""
        candidates = set()
        for key in self.index_blocks:
            if self.index_blocks[key].contains(index):
                candidates.add((len(self.index_blocks[key].index), key))
        if len(candidates) > 0:
            return candidates.pop()[1]
        key = hash_index(index)
        while (key in self.indices) or (key in self.index_blocks):
            key += '1'
        self.indices[key] = index
        self.index_blocks[key] = IndexBlock(index_name=key, alias_names=[], name=key)
        self.index_blocks_encoded[key] = self.index_blocks[key].to_dict()
        return key

    def _get_index_reference(self, index: pd.Index) -> str:
        """Gets index of a given dataframe as a reference to a value stored in dictionary."""
        for key in self.indices:
            if len(self.indices[key]) == len(index) and all(self.indices[key] == index):
                return key
        key = hash_index(index)
        assert key not in self.indices
        self.indices[key] = index
        return key

    def concat(self, dfs: List[Union[DataFrameAlias, KTSFrame, pd.DataFrame]]) -> pd.DataFrame:
        """Concatenates dataframes or their aliases. Preserves column order if no duplicate column names are passed.

        Will fail on duplicate columns. Needs redesign and optimization.

        Crash case:
            ```
            df = pd.DataFrame({'a': (1, 2), 'b': (1, 2)})
            pd.concat([df, df, df], axis=1)[['a', 'b', 'a', 'b', 'a', 'b']]
            ```

        Current solution: group columns with same names.
        """
        aliases = [i for i in dfs if isinstance(i, DataFrameAlias)]
        frames = [i for i in dfs if i not in aliases]
        res_frames = []
        res_alias = None
        if len(aliases) > 0:
            res_alias = aliases[0]
            for alias in aliases[1:]:
                res_alias = res_alias.join(alias)
        if res_alias is not None:
            res_frames.append(res_alias.data)
        if len(frames) > 0:
            common_index = frames[0].index
            for frame in frames[1:]:
                common_index = common_index.intersection(frame.index)
            if len(res_frames) > 0:
                common_index = common_index.intersection(res_frames[0].index)
                res_frames[0] = res_frames[0].loc[common_index]
            res_frames += [f.loc[common_index] for f in frames]
        res_columns_dups = sum([df.columns for df in dfs], [])
        res_columns = list()
        for col in res_columns_dups:
            if col not in res_columns:
                res_columns.append(col)
        res_frame = pd.concat(res_frames, axis=1)  # weak lines: concat will leave columns with same names 
        return res_frame[res_columns]              # and df[cols] will gather identically-named columns

frame_cache = FrameCache()
