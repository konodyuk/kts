from . import cache_utils
from .. import config
from glob import glob


class Info:
    def __init__(self):
        super().__setattr__('__attributes', dict())
        self.recalc()

    def recalc(self):
        for name in glob(config.info_path + '*'):
            super().__getattribute__('__attributes')[name.split('/')[-1][:-5]] = cache_utils.load_obj(name)
            
    def __setattr__(self, key, value):
        self.__attributes[key] = value
        cache_utils.save_obj(value, cache_utils.get_path_info(key))
    
    def __getattr__(self, key):
        self.recalc()
        if key in self.__attributes:
            return self.__attributes[key]
        else:
            raise KeyError

    def __delattr__(self, key):
        raise AttributeError("info is read-only")
            
    def __del__(self):
        for key, value in self.__attributes.items():
            print(key, value)
            cache_utils.save_obj(value, cache_utils.get_path_info(key))


info = Info()
