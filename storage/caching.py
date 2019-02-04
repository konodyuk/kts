from . import utils
from .. import config
import os
import glob


class Cache:
    """
    Standard interface for caching DataFrames and objects
    """

    def __init__(self):
        self.memory = dict()

    def cache_df(self, df, name):
        """
        Caches dataframe with given name
        :param df: object
        :param name: object name
        :return:
        """
        dict_name = name + '_df'
        self.memory[dict_name] = df
        utils.save_df(df, utils.get_path_df(name))

    def load_df(self, name):
        """
        Loads dataframe from cache
        :param name: name of object
        :return: dataframe with given name
        """
        dict_name = name + '_df'
        if dict_name in self.memory:
            return self.memory[dict_name]
        elif os.path.exists(utils.get_path_df(name)):
            tmp = utils.load_df(utils.get_path_df(name))
            self.memory[dict_name] = tmp
            return tmp
        else:
            raise KeyError("No such df in cache")

    def is_cached_df(self, name):
        """
        Checks whether df is in cache
        :param name: name of frame
        :return: True or False (cache hit or miss)
        """
        dict_name = name + '_df'
        return dict_name in self.memory or os.path.exists(utils.get_path_df(name))

    def cache_obj(self, obj, name):
        """
        Caches object with given name
        :param obj: object
        :param name: object name
        :return:
        """
        dict_name = name + '_obj'
        self.memory[dict_name] = obj
        utils.save_obj(obj, utils.get_path_obj(name))

    def load_obj(self, name):
        """
        Loads object from cache
        :param name: name of object
        :return: object with given name
        """
        dict_name = name + '_obj'
        if dict_name in self.memory:
            return self.memory[dict_name]
        elif os.path.exists(utils.get_path_obj(name)):
            tmp = utils.load_obj(utils.get_path_obj(name))
            self.memory[dict_name] = tmp
            return tmp
        else:
            raise KeyError("No such object in cache")

    def is_cached_obj(self, name):
        """
        Checks whether obj is in cache
        :param name: name of file
        :return: True or False (chache hit or miss)
        """
        dict_name = name + '_obj'
        return dict_name in self.memory or os.path.exists(utils.get_path_obj(name))


cache = Cache()
