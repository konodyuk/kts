from .. import config

class Validator:
    def __init__(self, df, y, features_before, features_after, single_splitter, n_folds=5, n_splits=3):
        self.df = df
        self.y = y
        self.features_before = features_before
        self.features_after = features_after
        self.single_splitter = single_splitter
        self.split = [single_splitter(y, n_folds, config.seeds[i]) for i in range(n_splits)]
        
    def score(model):
        for idx_split in range(self.n_splits):
            for idx_fold, (idx_train, idx_test) in self.split[idx_split]:
                model.fit()