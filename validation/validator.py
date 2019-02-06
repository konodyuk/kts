import numpy as np

from .. import config
from .experiment import experiment_list, Experiment
from copy import deepcopy

class Validator:
    def __init__(self, splitter, metric):
        self.splitter = splitter
        self.metric = metric
        
    def score(self, model, featureset, target):
        models = []
        scores = []
        oofs = np.zeros_like(target, dtype=np.float)
        weights = np.zeros_like(target, dtype=np.float)
        for spl in self.splitter.split:
            idx_train = spl['train']
            idx_test = spl['test']
            c_model = deepcopy(model)
            c_model.fit(featureset[idx_train], target[idx_train])
            pred = c_model.predict(featureset[idx_test])
            oofs[idx_test] = (weights[idx_test] * oofs[idx_test] + pred) / (weights[idx_test] + 1)
            weights[idx_test] += 1
            score = self.metric(target[idx_test], pred)
            models.append(c_model)
            scores.append(score)
        model_name = f"ens_{models[0].__name__}_x{len(models)}"
        final_ensemble = models[0]
        for model in models[1:]:
            final_ensemble = final_ensemble + model
        final_ensemble = final_ensemble / len(models)
        final_ensemble.__name__ = model_name
        score = np.mean(scores)
        std = np.std(scores)
        experiment_list.register(
            Experiment(
                final_ensemble,
                featureset,
                oofs,
                score,
                std
            )
        )
        return score