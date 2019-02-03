from .. import config
from . import utils

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
        self.splits.add(split)
        self.save()
    
    def status(self, split):
        for spl in self.splits:
            if set(spl['train']) == set(split):
                return 'train'
            elif set(spl['test']) == set(split):
                return 'test'
        return 'unknown'
    
split_manager = SplitManager()