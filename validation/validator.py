import numpy as np

from .. import config
from copy import deepcopy

class Validator:
    def __init__(self, featureset, target, splitter, metric):
        self.fs = featureset
        self.target = target
        self.splitter = splitter
        self.metric = metric
        
    def score(self, model):
        models = []
        scores = []
        oofs = np.zeros_like(self.target)
        weights = np.zeros_like(self.target)
        for spl in self.splitter.split:
            idx_train = spl['train']
            idx_test = spl['test']
            c_model = deepcopy(model)
            c_model.fit(self.fs[idx_train], self.target[idx_train])
            pred = c_model.predict(self.fs[idx_test])
            oofs[idx_test] = (weights[idx_test] * oofs[idx_test] + pred) / (weights[idx_test] + 1)
            weights[idx_test] += 1
            score = self.metric(self.target[idx_test], pred)
            models.append(model)
            scores.append(score)
        return np.mean(scores)