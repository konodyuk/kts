from .. import config
from copy import deepcopy

class Validator:
    def __init__(self, df, y, splitter):
        self.df = df
        self.y = y
        self.splitter = splitter
        
    def score(model):
        df = model.preprocess_before_split(self.df)
        raw_model = deepcopy(model)
        for split in self.splitter.split:
            idx_train = split['train']
            idx_test = split['test']
            df_train = df.iloc[idx_train]
            y_train = y[idx_train]
            df_test = df.iloc[idx_test]
            y_test = y[idx_test]
            cur_model = deepcopy(raw_model)
            cur_model.fit(df_train, y_train)
            y_hat = cur_model.predict(df_test)
            
                