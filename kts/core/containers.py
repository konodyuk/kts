from collections.abc import MutableMapping
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
