from . import cache_utils
from .. import config
import datetime
from glob import glob
import os
from .info import info
import pandas as pd
from .dataframe import DataFrame as KTDF

class Cache:
    """
    LRU cache for DataFrames and objects
    """

    def __init__(self):
        self.memory = dict()
        self.last_used = dict()
        self.edited_at = dict()
        self.current_volume = 0
        try:
            _ = info.memory_limit  # to check whether it exists
        except Exception:
            info.memory_limit = 4 * (1024 ** 3)  # 4 Gb

    @staticmethod
    def set_memory_limit(volume):
        """
        Sets a new memory limit in bytes
        :param volume: new memory limit
        :return:
        """
        info.memory_limit = volume

    def __release_volume(self, df):
        """
        Removes most unpopular dataframes until it is possible to cache given one
        :param df: dataframe
        :return:
        """

        items = sorted([(time, key) for (key, time) in self.last_used.items()])
        cur = 0
        while self.current_volume + cache_utils.get_df_volume(df) > info.memory_limit:
            key = items[cur][1]
            cur += 1
            self.current_volume -= cache_utils.get_df_volume(self.memory[key])
            self.memory.pop(key)
            self.last_used.pop(key)
            self.edited_at.pop(key)

    def is_cached_df(self, name):
        """
        Checks whether given df is cached
        :param name: name of dataframe
        :return: True or False (cache hit or miss)
        """
        # dict_name = name + '_df'
        # return dict_name in self.memory or os.path.exists(cache_utils.get_path_df(name))
        return os.path.exists(cache_utils.get_path_df(name))

    def cache_df(self, df, name):
        """
        Caches dataframe with given name
        :param df: df
        :param name: df name
        :return:
        """
        if self.is_cached_df(name):
            return
        if cache_utils.get_df_volume(df) > info.memory_limit:
            raise MemoryError

        dict_name = name + '_df'
        self.__release_volume(df)
        cache_utils.save_df(df, cache_utils.get_path_df(name))
        self.memory[dict_name] = df
        self.current_volume += cache_utils.get_df_volume(df)
        self.last_used[dict_name] = datetime.datetime.now()
        self.edited_at[dict_name] = cache_utils.get_time(cache_utils.get_path_df(name))

    def load_df(self, name):
        """
        Loads dataframe from cache
        :param name: name of df
        :return:
        """
        if not self.is_cached_df(name):
            raise KeyError("No such df in cache")

        dict_name = name + '_df'
        self.last_used[dict_name] = datetime.datetime.now()
        if dict_name in self.memory:
            if self.edited_at[dict_name] != cache_utils.get_time(cache_utils.get_path_df(name)):
                self.memory[dict_name] = cache_utils.load_df(cache_utils.get_path_df(name))
                self.edited_at[dict_name] = cache_utils.get_time(cache_utils.get_path_df(name))
            return self.memory[dict_name]
        else:
            tmp = cache_utils.load_df(cache_utils.get_path_df(name))
            self.__release_volume(tmp)
            self.memory[dict_name] = tmp
            self.edited_at[dict_name] = cache_utils.get_time(cache_utils.get_path_df(name))
            self.current_volume += cache_utils.get_df_volume(tmp)
            return tmp

    def remove_df(self, name):
        """
        Removes dataframe from cache
        :param name: name of df
        :return:
        """
        dict_name = name + '_df'
        if dict_name in self.memory:
            self.current_volume -= cache_utils.get_df_volume(self.memory[dict_name])
            self.memory.pop(dict_name)
        if dict_name in self.last_used:
            self.last_used.pop(dict_name)
        if dict_name in self.edited_at:
            self.edited_at.pop(dict_name)
        if os.path.exists(cache_utils.get_path_df(name)):
            os.remove(cache_utils.get_path_df(name))

    @staticmethod
    def cached_dfs():
        """
        Returns list of cached dataframes
        :return:
        """
        return [df.split('/')[-1][:-3] for df in
                sorted(glob(config.storage_path + '*' + '_df'), key=os.path.getmtime)]


    def is_cached_obj(self, name):
        """
        Checks whether object is in cache
        :param name: name of object
        :return: True or False (cache hit or miss)
        """
        # dict_name = name + '_obj'
        # return dict_name in self.memory or os.path.exists(cache_utils.get_path_obj(name))
        return os.path.exists(cache_utils.get_path_obj(name))

    def cache_obj(self, obj, name):
        """
        Caches object with given name
        :param obj: object
        :param name: object name
        :return:
        """
        if self.is_cached_obj(name):
            return

        dict_name = name + '_obj'
        self.memory[dict_name] = obj
        cache_utils.save_obj(obj, cache_utils.get_path_obj(name))
        self.edited_at[dict_name] = cache_utils.get_time(cache_utils.get_path_obj(name))

    def load_obj(self, name):
        """
        Loads object from cache
        :param name: name of object
        :return:
        """
        if not self.is_cached_obj(name):
            raise KeyError("No such object in cache")

        dict_name = name + '_obj'
        if dict_name in self.memory:
            if self.edited_at[dict_name] != cache_utils.get_time(cache_utils.get_path_obj(name)):
                self.memory[dict_name] = cache_utils.load_obj(cache_utils.get_path_obj(name))
                self.edited_at[dict_name] = cache_utils.get_time(cache_utils.get_path_obj(name))
            return self.memory[dict_name]
        else:
            tmp = cache_utils.load_obj(cache_utils.get_path_obj(name))
            self.memory[dict_name] = tmp
            self.edited_at[dict_name] = cache_utils.get_time(cache_utils.get_path_obj(name))
            return tmp

    def remove_obj(self, name):
        """
        Removes object from cache
        :param name: name of object
        :return:
        """
        dict_name = name + '_obj'
        if dict_name in self.memory:
            self.memory.pop(dict_name)
        if dict_name in self.last_used:
            self.last_used.pop(dict_name)
        if dict_name in self.edited_at:
            self.edited_at.pop(dict_name)
        if os.path.exists(cache_utils.get_path_obj(name)):
            os.remove(cache_utils.get_path_obj(name))

    @staticmethod
    def cached_objs():
        """
        Returns list of cached objects
        :return:
        """
        return [df.split('/')[-1][:-4] for df in
                sorted(glob(config.storage_path + '*' + '_obj'), key=os.path.getmtime)]


cache = Cache()


USER_SEP = '__USER__'


def save(obj, name):
    if name in ls():
        raise KeyError("You've already saved object with this name. If you want to overwrite it, first use kts.rm()")
    if isinstance(obj, KTDF) or isinstance(obj, pd.DataFrame):
        cache.cache_df(obj, name + USER_SEP)
    else:
        cache.cache_obj(obj, name + USER_SEP)


def ls():
    return [df.split('/')[-1][:-4 - len(USER_SEP)] for df in
            sorted(glob(config.storage_path + '*' + USER_SEP + '_obj'), key=os.path.getmtime)
           ] + [df.split('/')[-1][:-3 - len(USER_SEP)] for df in
            sorted(glob(config.storage_path + '*' + USER_SEP + '_df'), key=os.path.getmtime)]


def get_type(name):
    if cache.is_cached_obj(name + USER_SEP):
        return 'obj'
    else:
        return 'df'


def load(name):
    if name not in ls():
        raise KeyError("No such object in cache")
    if get_type(name) == 'df':
        return cache.load_df(name + USER_SEP)
    else:
        return cache.load_obj(name + USER_SEP)


def remove(name):
    if name not in ls():
        return
    if get_type(name) == 'df':
        cache.remove_df(name + USER_SEP)
    else:
        cache.remove_obj(name + USER_SEP)


rm = remove
