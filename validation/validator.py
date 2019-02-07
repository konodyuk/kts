import numpy as np

from .. import config
from .experiment import experiment_list, Experiment
from ..pipeline import Pipeline
# from ..feature.storage import FeatureSlice
from copy import deepcopy

class Validator:
    def __init__(self, splitter, metric):
        self.splitter = splitter
        self.metric = metric
        
    def score(self, model, featureset):
        pipelines = []
        scores = []
        oofs = np.zeros_like(featureset.target, dtype=np.float)
        weights = np.zeros_like(featureset.target, dtype=np.float)
        for spl in self.splitter.split:
            idx_train = spl['train']
            idx_test = spl['test']
            c_model = deepcopy(model)
            pl = Pipeline(c_model, featureset.slice(idx_train))
            pl.fit()
            pred = pl.predict(featureset.df_input.iloc[idx_test])  # CODESTYLE: looks like rubbish
            oofs[idx_test] = (weights[idx_test] * oofs[idx_test] + pred) / (weights[idx_test] + 1)
            weights[idx_test] += 1
            # print(featureset.target[idx_test].values[:10], pred[:10])
            score = self.metric(featureset.target.values[idx_test], pred)
            pipelines.append(pl)
            scores.append(score)
        model_name = f"ens_{pipelines[0].model.__name__}_x{len(pipelines)}"
        final_ensemble = pipelines[0]
        for pipeline in pipelines[1:]:
            final_ensemble = final_ensemble + pipeline
        final_ensemble = final_ensemble / len(pipelines)
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