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
            
    @property
    def size(self):
        return self.n_folds
            
        
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
            
            
    @property
    def size(self):
        return self.n_folds * self.n_splits


from sklearn.model_selection import train_test_split
class Holdout(BaseSplitter):
    def __init__(self, y, test_size=0.3):
        self.sz = len(y)
        self.test_size = test_size

    def _split(self):
        idx_train, idx_test = train_test_split(np.arange(self.sz), test_size=self.test_size, shuffle=True, random_state=config.seed)
        yield {'train': idx_train, 'test': idx_test}

    @property
    def size(self):
        return 1