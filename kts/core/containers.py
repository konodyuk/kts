from collections import MutableSequence, MutableMapping
from typing import Any

from kts.core.cache import obj_cache


class CachedMapping(MutableMapping):
    def __init__(self, name):
        self.name = name

    def __create_name__(self, key):
        return f"M.{self.name}.{key}"

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
