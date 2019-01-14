import numpy as np

class BaseSplitter:
    def __init__(self, y, n_splits, seed):
        self.y = y
        self.n_splits = n_splits
        self.seed = seed
        self.split_memoized
        
    @property
    def split(self):
        yield from self._split()
            
        
from sklearn.model_selection import StratifiedKFold
class StratifiedKFold(BaseSplitter):
    def _split(self):
        yield from StratifiedKFold(self.n_splits, shuffle=True, random_state=self.seed).split(np.zeros(len(y)), y)
        
        
from sklearn.model_selection import KFold
class KFold(BaseSplitter):
    def _split(self):
        yield from KFold(self.n_splits, shuffle=True, random_state=self.seed).split(np.zeros(len(y)), y)