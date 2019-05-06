from .. import config
import numpy as np

class BaseSplitter:
    def __init__(self, y, n_folds=5, seed=config.seed):
        raise DeprecationWarning('Use splitters from sklearn')
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
        raise DeprecationWarning('Use splitters from sklearn')
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


from sklearn.model_selection import ShuffleSplit
from sklearn.model_selection import StratifiedShuffleSplit
class Holdout(BaseSplitter):
    def __init__(self, y, test_size=0.3, stratify=False):
        raise DeprecationWarning('Use splitters from sklearn')
        self.sz = len(y)
        self.y = y
        self.test_size = test_size
        if stratify:
            self.Splitter = StratifiedShuffleSplit
        else:
            self.Splitter = ShuffleSplit

    def _split(self):
        for idx_train, idx_test in self.Splitter(
            n_splits=1,
            test_size=self.test_size,
            random_state=self.seed
        ).split(np.zeros(len(self.y)), self.y):
            yield {'train': idx_train, 'test': idx_test}

    @property
    def size(self):
        return 1


# BUG: when using LOO split, we should score the final OOF-preds, but our current Validator will score
# each "1-sample-fold" separately using given metric instead of aggregating the predictions and scoring them all at once
# A possible solution is to set a flag "score_oofs=True" for LOO split and check it inside of Validator
from sklearn.model_selection import LeaveOneOut
class LOO(BaseSplitter):
    def __init__(self):
        pass

    def _split(self):
        for idx_train, idx_test in LeaveOneOut().split(np.zeros(len(self.y)), self.y):
            yield {'train': idx_train, 'test': idx_test}