from . import utils
from .. import config
import os
import glob


class Cache:
    '''
    Doesn't work yet
    '''
    def __init__(self):
        self.memory = dict()
    
    def cache_df(self, df, name):
        self.memory[name] = df
        utils.save_df(df, utils.get_path_df(name))
        
    def load_df(self, name):
        if name in self.memory:
            return self.memory[name]
        elif os.path.exists(utils.get_path_df(name)):
            self.memory[name] = utils.load_df(utils.get_path_df(name))
            return self.memory[name]
        else:
            raise KeyError("No such df in cache")

    def is_cached_df(self, name):
        return name in self.memory or os.path.exists(utils.get_path_df(name))

    def cache_obj(self, obj, name):
        self.memory[name] = obj
        utils.save_obj(obj, utils.get_path_obj(name))

    def load_obj(self, name):
        if name in self.memory:
            return self.memory[name]
        elif os.path.exists(utils.get_path_obj(name)):
            self.memory[name] = utils.load_obj(utils.get_path_obj(name))
            return self.memory[name]
        else:
            raise KeyError("No such object in cache")

    def is_cached_obj(self, name):
        return name in self.memory or os,path.exists(utils.get_path_obj(name))


cache = Cache()
