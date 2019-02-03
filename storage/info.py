from . import utils
from .. import config
import os
from glob import glob

class Info:
    def __init__(self):
        super().__setattr__('attributes', dict())
        #print(self.attributes)
        for name in glob(config.info_path + '*'):
            #print(name)
            self.attributes[name.split('/')[-1]] = utils.load_info(name)
            
    def __setattr__(self, key, value):
        self.attributes[key] = value
        utils.save_info(value, utils.get_path_info(key))
    
    def __getattr__(self, key):
        if key in self.attributes:
            return self.attributes[key]
        else:
            raise KeyError
            
    def __del__(self):
        #print('del called')
        for key, value in self.attributes.items():
            print(key, value)
            utils.save_info(value, utils.get_path_info(key))
            
info = Info()
