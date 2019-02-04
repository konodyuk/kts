from .. import config
from . import utils
import os

class SplitManager:
    def __init__(self):
        self.splits = set()
        if os.path.exists(utils.get_path_info('splits')):
            self.load()
    
    def load(self):
        self.splits = utils.load_info(utils.get_path_info('splits'))
    
    def save(self):
        utils.save_info(self.splits, utils.get_path_info('splits'))
    
    def register(self, split):
        if self.status(split) == 'unknown':
            self.splits.add(split)
            self.save()
    
    def status(self, split):
        for spl in self.splits:
            if frozenset(spl['train']) == frozenset(split):
                return 'train'
            elif frozenset(spl['test']) == frozenset(split):
                return 'test'
        return 'unknown'
    
split_manager = SplitManager()