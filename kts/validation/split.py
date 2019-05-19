import numpy as np
from sklearn.model_selection import BaseCrossValidator
from sklearn.utils import indexable


class Refiner(BaseCrossValidator):
    def __init__(self, outer_splitter, inner_splitter=None):
        super().__init__()
        self.outer_splitter = outer_splitter
        self.inner_splitter = (inner_splitter if inner_splitter is not None else outer_splitter)

    def split(self, X, y=None, groups=None):
        X, y, groups = indexable(X, y, groups)
        print(groups)
        for _, idx_test_outer in self.outer_splitter.split(X, y, groups):
            for idx_train, idx_test in self.inner_splitter.split(X[idx_test_outer],
                                                                 y[idx_test_outer] if y is not None else None,
                                                                 groups[idx_test_outer] if groups is not None else None):
                yield idx_test_outer[idx_train], idx_test_outer[idx_test]

    def get_n_splits(self, X=None, y=None, groups=None):
        return self.outer_splitter.get_n_splits() * self.inner_splitter.get_n_splits()
