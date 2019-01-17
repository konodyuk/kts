from .. import config
import numpy as np

class BaseSplitter:
    def __init__(self, y, n_folds, seed):
        self.y = y
        self.n_folds = n_folds
        self.seed = seed
        
    @property
    def split(self):
        yield from self._split()
            
        
from sklearn.model_selection import StratifiedKFold
class SKF(BaseSplitter):
    def _split(self):
        for idx_train, idx_test in StratifiedKFold(
            self.n_folds, 
            shuffle=True, 
            random_state=self.seed
        ).split(np.zeros(len(self.y)), self.y):
            yield {'train': idx_train, 'test': idx_test}
            
        
        
from sklearn.model_selection import KFold
class KF(BaseSplitter):
    def _split(self):
        for idx_train, idx_test in KFold(
            self.n_folds, 
            shuffle=True, 
            random_state=self.seed
        ).split(np.zeros(len(self.y)), self.y):
            yield {'train': idx_train, 'test': idx_test}
            

class NM(BaseSplitter):
    def __init__(self, y, Splitter, n_folds=5, n_splits=3):
        self.y = y
        self.Splitter = Splitter
        self.n_folds = n_folds
        self.n_splits = n_splits
      
    def _split(self):
        for i in range(self.n_splits):
            yield from self.Splitter(self.y, 
                                     self.n_folds,
                                     config.seeds[i]).split